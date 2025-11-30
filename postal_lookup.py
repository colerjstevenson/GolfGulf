from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

import csv
from collections import defaultdict
import os

class PostalCodeLookup:
    def __init__(self, user_agent="postal_code_lookup"):
        self.geolocator = Nominatim(user_agent=user_agent)
    
    def _match_by_course_name(self, coords_row, info_list):
        """
        Attempt to find a matching course in info_list by comparing course names.
        Returns the best matching info row, or None if no match found.
        
        Uses a similarity check based on:
        1. Course name matching (primary) - ignoring common golf course words
        2. State/Province matching (secondary)
        3. City matching (tertiary)
        """
        if not info_list or "name" not in coords_row:
            return None
        
        coords_name = coords_row.get("name", "").lower().strip()
        if not coords_name:
            return None
        
        coords_city = coords_row.get("city", "").lower().strip() if "city" in coords_row else ""
        coords_state = coords_row.get("state", "").lower().strip() if "state" in coords_row else ""
        
        # Common words to exclude from name matching (they appear in most golf courses)
        common_words = {"golf", "course", "club", "country", "cc", "public", "private", "municipal"}
        
        def filter_words(name_str):
            """Remove common golf course words and return filtered word set."""
            words = set(name_str.split())
            return words - common_words
        
        coords_words = filter_words(coords_name)
        
        candidates = []
        
        for info_row in info_list:
            info_name = info_row.get("CourseName", "").lower().strip()
            if not info_name or info_name == "nomatch":
                continue
            
            info_words = filter_words(info_name)
            
            # Skip if either has no meaningful words after filtering
            if not coords_words or not info_words:
                continue
            
            # Calculate name similarity: check if words match or if one is substring of other
            matching_words = len(coords_words & info_words)
            total_words = len(coords_words | info_words)
            
            name_score = matching_words / total_words if total_words > 0 else 0
            
            # Also check for substring matches (using original names)
            if coords_name in info_name or info_name in coords_name:
                name_score = max(name_score, 0.8)
            
            # Only consider rows with reasonable name confidence
            if name_score >= 0.5:
                # Calculate location bonus scores
                location_bonus = 0
                
                info_city = info_row.get("City", "").lower().strip()
                info_state = info_row.get("State", "").lower().strip()
                
                # State/Province match (most important)
                if coords_state and info_state and (coords_state in info_state or info_state in coords_state):
                    location_bonus += 0.3
                
                # City match (second most important)
                if coords_city and info_city and (coords_city in info_city or info_city in coords_city):
                    location_bonus += 0.2
                
                total_score = name_score + location_bonus
                candidates.append((total_score, info_row))
        
        # Return the best match if any candidates found
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            return candidates[0][1]
        
        return None
    
    def greedy_match_by_postal(self, coords_csv, info_csv, output_csv):
        # Load coordinates table (A)
        coords_by_postal = defaultdict(list)
        print("Loading coordinates...")
        with open(coords_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Load postal codes mapping once (from a postal_codes.csv located alongside coords_csv)
                if 'postal_map' not in locals():
                    postal_map = {}
                    postal_file = os.path.join(os.path.dirname(coords_csv), "postal_codes.csv")
                    try:
                        with open(postal_file, newline="", encoding="utf-8") as pf:
                            preader = csv.DictReader(pf)
                            for prow in preader:
                                key = prow.get("gcid")
                                val = prow.get("postal_code", "")
                                if key and val:
                                    postal_map[key] = val.replace(" ", "").upper()
                    except Exception:
                        # If postal file can't be read, keep postal_map empty and fallback to NOMATCH
                        postal_map = {}

                gcid = row.get("gcid")
                # Prefer lookup from postal_map by gcid; fall back to any postal field in the row if present
                postal = None
                if gcid:
                    postal = postal_map.get(gcid)
                if not postal:
                    postal = (row.get("postal_code") or row.get("postal") or "").replace(" ", "").upper() if (row.get("postal_code") or row.get("postal")) else None
                if postal:
                    coords_by_postal[postal].append(row)
                else:
                    print(f"  Warning: could not find postal code for {row['gcid']}. Skipping.")

        # Load info table (B)
        info_by_postal = defaultdict(list)
        print("Loading golf link...")
        with open(info_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                postal = row["Zip"].replace(" ", "").upper()
                if postal in coords_by_postal:
                    info_by_postal[postal].append(row)
        
        # Also load all info data globally for name-based fallback matching
        all_info_data = []
        print("Loading all info data for fallback name matching...")
        with open(info_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            all_info_data = list(reader)

        # extra_by_postal = defaultdict(list)
        # print("Loading golf canada...")
        # with open('data/canada/golf_canada_full.csv', newline="", encoding="utf-8") as f:
        #     reader = csv.DictReader(f)
        #     for row in reader:
        #         postal = row["postal_code"].replace(" ", "").upper()
        #         if postal in coords_by_postal:
        #             entry = row
        #             entry["AccessType"] = entry.get("Course Type", "NOMATCH")
        #             entry["CourseName"] = "NOMATCH"
        #             entry["NumHoles"] = entry.get("# of holes", "NOMATCH")
        #             entry["Par"] = entry.get("Course Par", "NOMATCH")
        #             entry["Address"] = entry.get("address", "NOMATCH")
        #             entry["City"] = entry.pop("location", "NOMATCH").split(", ")[0] if ", " in entry.get("location", "") else entry.get("location", "NOMATCH")
        #             entry["State"] = entry.pop("locations", "NOMATCH").split(", ")[1] if ", " in entry.get("locations", "") else "NOMATCH"
        #             entry["Yardage"] = entry.pop("Total Yards", "NOMATCH")
        #             entry["established"] = "NOMATCH"
        #             entry["url"] = entry.pop("url", "NOMATCH")
        #             entry["website"] = entry.pop("Website", "NOMATCH")

        #             info_by_postal[postal].append(row)

        # Prepare output
        fieldnames = [
                    "gcid",
                    "latitude",
                    "longitude",
                    "area_m2",
                    "AccessType",
                    "Address",
                    "City",
                    "postal_code",
                    "CourseName",
                    "NumHoles",
                    "Par",
                    "Phone",
                    "Region",
                    "Yardage",
                    "established",
                    "url",
                    "website",
                    "match_type"
                    ]
        print("Writing output...")
        with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()

            for postal in set(coords_by_postal):
                print(f"Processing postal code: {postal}")
                coords_list = coords_by_postal.get(postal, [])
                info_list = info_by_postal.get(postal, [])
                
                # if len(info_list) == 0:
                #     info_list = extra_by_postal.get(postal, [])
                        
                #     if len(info_list) > 0:
                #         print(f"  Found {len(info_list)} additional info entries from golf canada for postal code {postal}.")
                

                lenA = len(coords_list)
                lenB = len(info_list)
                
                if lenA == 0:
                    print(f"  No coordinates found for postal code {postal}, skipping.")
                    continue
                
                if lenB == 0 or postal == "NOMATCH":
                    print(f"  No info found for postal code {postal}, adding NOMATCH entries.")
                    info_list.append({
                        "CourseName": "NOMATCH",
                        "NumHoles": "NOMATCH",
                        "Par": "NOMATCH",
                        "Phone": "NOMATCH",
                        "Address": "NOMATCH",
                        "City": "NOMATCH",
                        "State": "NOMATCH",
                        "Yardage": "NOMATCH",
                        "established": "NOMATCH",
                        "url": "NOMATCH",
                        "website": "NOMATCH"
                    })
                    lenB = 1
                    

                n = max(lenA, lenB)
                for i in range(n):
                    a = coords_list[i%(lenA)]  # wrap around if needed
                    b = info_list[i%(lenB)]  # wrap around if needed
                    
                    # Try to find a better match by course name if:
                    # 1. There are multiple courses in coordinates but only one in info (ambiguous)
                    # 2. There are no info matches for this postal code (name-based fallback)
                    if (lenA > 1 and lenB == 1 and info_list[0].get("CourseName") != "NOMATCH") or \
                       (lenB == 1 and info_list[0].get("CourseName") == "NOMATCH"):
                        name_match = self._match_by_course_name(a, all_info_data)
                        if name_match:
                            b = name_match
                            print(f"    Found name-based match for {a.get('name', 'Unknown')}: {b.get('CourseName', 'Unknown')}")
                    
                    
                    if lenA == 1 and lenB == 1:
                        match_type = "unique"
                    elif lenA > lenB:
                        match_type = "multiple_coords_more"
                    elif lenB > lenA:
                        match_type = "multiple_info_more"
                    else:
                        match_type = "multiple"
                    
                    writer.writerow({
                        "postal_code": postal,
                        "gcid": a["gcid"],
                        "latitude": a["lat"],
                        "longitude": a["lon"],
                        "area_m2": a["area_m2"],
                        "AccessType": b.get("AccessType", "NOMATCH"),
                        "CourseName": b["CourseName"],
                        "NumHoles": b["NumHoles"],
                        "Par": b["Par"],
                        "Phone": b["Phone"],
                        "Address": b["Address"],
                        "City": b["City"],
                        "Region": b["State"],
                        "Yardage": b["Yardage"],
                        "established": b["established"],
                        "url": b["url"],
                        "website": b["website"],
                        "match_type": match_type
                    })

               

    def get_postal_code(self, latitude, longitude):
        """
        Returns the postal code (ZIP code in the US) for a given lat/lon pair.

        Returns:
            postal_code (str) or None if not found.
        """
        try:
            location = self.geolocator.reverse((latitude, longitude), addressdetails=True)
        except (GeocoderTimedOut, GeocoderUnavailable):
            return None

        if not location or "address" not in location.raw:
            return None

        address = location.raw["address"]

        # Postal code fields differ slightly by country but "postcode" is universal
        postal_code = address.get("postcode")
        return postal_code
    
    def add_postal_codes(self, coords_csv, postal_codes_csv):
        """
        Ensure every GCID in coords_csv has a postal code in postal_codes_csv.
        If missing, look up postal code and append to postal_codes_csv.
        """
        print("Loading coordinates...")
        coords = {}
        with open(coords_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                gcid = row["gcid"]
                coords[gcid] = (row.get("lat"), row.get("lon"), row)

        print("Loading existing postal codes...")
        postal_codes = {}
        with open(postal_codes_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                postal_codes[row["gcid"]] = row["postal_code"]

        missing_gcids = [gcid for gcid in coords if gcid not in postal_codes]
        print(f"Found {len(missing_gcids)} GCIDs missing postal codes.")

        new_rows = []
        for i, gcid in enumerate(missing_gcids):
            lat, lon, row = coords[gcid]
            print(f"[{i+1}/{len(missing_gcids)}] Looking up postal code for GCID: {gcid} ({row.get('name', '')})")
            try:
                lat_f = float(lat) if lat else None
                lon_f = float(lon) if lon else None
            except Exception:
                lat_f = lon_f = None
            postal_code = self.get_postal_code(lat_f, lon_f) if lat_f and lon_f else "NOMATCH"
            print(f"  Found postal code: {postal_code}")
            new_rows.append({"gcid": gcid, "postal_code": postal_code})

        # Append new postal codes to the file
        if new_rows:
            print(f"Appending {len(new_rows)} new postal codes to {postal_codes_csv}...")
            with open(postal_codes_csv, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["gcid", "postal_code"])
                # Only write header if file is empty
                if f.tell() == 0:
                    writer.writeheader()
                for row in new_rows:
                    writer.writerow(row)
        else:
            print("No missing GCIDs. Postal codes file is up to date.")


if __name__ == "__main__":
    COUNTRY = "world"
    # Example usage
    lookup = PostalCodeLookup()
    lookup.add_postal_codes(f"data/{COUNTRY}/combined.csv", f"data/{COUNTRY}/postal_codes.csv")
    lookup.greedy_match_by_postal(f"data/{COUNTRY}/combined.csv", "data/golfLinkData.csv", f"data/{COUNTRY}/Fully_Matched_Golf_Courses.csv")
