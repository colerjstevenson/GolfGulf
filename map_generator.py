"""
Golf Course Map Generator

This module reads golf course data from a CSV file and generates an interactive
map with pins at each unique latitude/longitude location.
"""

import pandas as pd
import folium
from folium.plugins import MarkerCluster
from pathlib import Path
import json
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import time
import os
from urllib.parse import urlparse
from datetime import datetime


# Global variables for API handler
csv_path_global = None


class CustomRequestHandler(SimpleHTTPRequestHandler):
    """Custom HTTP request handler to support API endpoints and CORS."""
    
    def do_POST(self):
        """Handle POST requests for API endpoints."""
        if self.path == '/api/update_row':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            try:
                data = json.loads(post_data.decode('utf-8'))
                row_idx = data.get('rowIdx')
                updates = data.get('updates', {})
                source_file = data.get('source_file')
                source_index = data.get('source_index')

                # Determine target CSV and row index
                if source_file:
                    target_csv = os.path.abspath(source_file)
                    target_row = int(source_index)
                else:
                    # fallback to global csv and provided row_idx
                    if isinstance(csv_path_global, (list, tuple)):
                        target_csv = csv_path_global[0]
                    else:
                        target_csv = csv_path_global
                    target_row = int(row_idx)

                # Save using helper to keep behavior consistent
                save_row_data(target_csv, target_row, updates)
                
                # Send success response
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True}).encode('utf-8'))
                print(f"Row {row_idx} updated successfully")
                
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def end_headers(self):
        """Add CORS headers to all responses."""
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def load_golf_courses(csv_path: str) -> pd.DataFrame:
    """
    Load golf course data from CSV file.
    
    Args:
        csv_path: Path to the CSV file containing golf course data
        
    Returns:
        DataFrame with golf course data
    """
    df = pd.read_csv(csv_path)
    # Ensure there's a column to track manual edits
    if 'manually_edited' not in df.columns:
        df['manually_edited'] = False
    return df


def load_multiple_csvs(csv_paths):
    """Load multiple CSV files and annotate rows with source info.

    Args:
        csv_paths: list of CSV file paths

    Returns:
        Combined DataFrame with additional columns: _source_file, _source_index
    """
    repo_root = Path(__file__).parent.resolve()
    frames = []
    for path in csv_paths:
        p = Path(path)
        df = pd.read_csv(p)
        
        # Standardize column names - handle both 'lat'/'lon' and 'latitude'/'longitude'
        if 'lat' in df.columns and 'latitude' not in df.columns:
            df['latitude'] = df['lat']
        if 'lon' in df.columns and 'longitude' not in df.columns:
            df['longitude'] = df['lon']
        
        # Ensure tracking column exists
        if 'manually_edited' not in df.columns:
            df['manually_edited'] = False

        # Compute relative path (relative to repo root)
        try:
            rel = str(p.resolve().relative_to(repo_root))
        except Exception:
            rel = os.path.relpath(str(p.resolve()), str(repo_root))

        # Annotate source file and original row index
        df['_source_file'] = rel
        df['_source_index'] = df.index.astype(int)

        frames.append(df)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = pd.DataFrame()

    return combined


def load_external_data(csv_path: str = "data/golfLinkData.csv") -> pd.DataFrame:
    """
    Load external golf course data from golfLinkData.csv.
    
    Args:
        csv_path: Path to external data CSV
        
    Returns:
        DataFrame with external data, or empty DataFrame if file not found
    """
    repo_root = Path(__file__).parent.resolve()
    full_path = repo_root / csv_path
    
    try:
        if full_path.exists():
            df = pd.read_csv(full_path, encoding='utf-8')
            return df
    except Exception as e:
        print(f"Warning: Could not load external data from {full_path}: {e}")
    
    return pd.DataFrame()


def save_row_data(csv_path: str, row_index: int, updated_data: dict):
    """
    Save updated row data back to the CSV file.
    
    Args:
        csv_path: Path to the CSV file
        row_index: Index of the row to update
        updated_data: Dictionary of updated column values
    """
    df = pd.read_csv(csv_path)
    # Add column if missing
    if 'manually_edited' not in df.columns:
        df['manually_edited'] = False

    for col, value in updated_data.items():
        if col in df.columns:
            df.at[row_index, col] = value if value != '' else None

    # Mark as manually edited with UTC ISO timestamp
    df.at[row_index, 'manually_edited'] = datetime.utcnow().isoformat()

    df.to_csv(csv_path, index=False)
    print(f"Row {row_index} updated and saved to {csv_path}")


def get_unique_locations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique latitude/longitude pairs from the dataframe.
    
    Args:
        df: DataFrame containing golf course data
        
    Returns:
        DataFrame with unique locations and associated course info
    """
    # Standardize column names - handle both 'lat'/'lon' and 'latitude'/'longitude'
    df = df.copy()
    if 'lat' in df.columns and 'latitude' not in df.columns:
        df['latitude'] = df['lat']
    if 'lon' in df.columns and 'longitude' not in df.columns:
        df['longitude'] = df['lon']
    
    # Remove rows with missing latitude or longitude
    df_clean = df.dropna(subset=['latitude', 'longitude'])
    
    # Get unique locations
    unique_locs = df_clean.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
    
    print(f"Total records: {len(df)}")
    print(f"Records with valid coordinates: {len(df_clean)}")
    print(f"Unique locations: {len(unique_locs)}")
    
    return unique_locs


def load_geojson_polygons(geojson_path: str) -> dict:
    """
    Load polygons from a GeoJSON file and index them by gcid.
    
    Args:
        geojson_path: Path to the GeoJSON file
        
    Returns:
        Dictionary mapping gcid -> geojson feature
    """
    geojson_map = {}
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        if 'features' in geojson_data:
            for feature in geojson_data['features']:
                if 'properties' in feature and 'gcid' in feature['properties']:
                    gcid = feature['properties']['gcid']
                    geojson_map[gcid] = feature
        
        print(f"Loaded {len(geojson_map)} polygons from {geojson_path}")
    except Exception as e:
        print(f"Error loading GeoJSON: {e}")
    
    return geojson_map


def create_golf_map(df: pd.DataFrame, unique_locations: pd.DataFrame, csv_path: str = None, output_file: str = "golf_courses_map.html") -> folium.Map:
    """
    Create an interactive map with golf course pins.
    
    Args:
        df: Full DataFrame with all data (needed for row lookups)
        unique_locations: DataFrame with unique golf course locations
        csv_path: Path to the original CSV file
        output_file: Name of the output HTML file
        
    Returns:
        Folium map object
    """
    # Calculate center of map based on the data
    center_lat = unique_locations['latitude'].mean()
    center_lon = unique_locations['longitude'].mean()
    
    print(f"Map center: ({center_lat:.4f}, {center_lon:.4f})")
    
    # Create base map
    golf_map = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=4,
        tiles='OpenStreetMap'
    )
    
    # Add marker cluster for better visualization at various zoom levels
    marker_cluster = MarkerCluster().add_to(golf_map)
    
    # Load GeoJSON polygons for all available regions
    repo_root = Path(__file__).parent.resolve()
    geojson_polygons = {}
    
    # Dynamically find all combined.geojson files in data subdirectories
    data_dir = repo_root / "data"
    if data_dir.exists():
        for geojson_file in data_dir.glob("*/combined.geojson"):
            region_geojson = load_geojson_polygons(str(geojson_file))
            geojson_polygons.update(region_geojson)
            print(f"Loaded polygons from {geojson_file.parent.name}: {len(region_geojson)} polygons")
    
    print(f"Total polygons loaded: {len(geojson_polygons)}")
    
    # Add individual markers
    for idx, row in unique_locations.iterrows():
        lat = row['latitude']
        lon = row['longitude']
        
        # Build popup text with all available information from the row
        popup_parts = []
        
        # Add all columns from the row as key-value pairs (skip internal source keys)
        for col, value in row.items():
            # Skip latitude/longitude as we already display them
            if col in ['latitude', 'longitude']:
                continue
            # Skip internal keys that start with underscore
            if isinstance(col, str) and col.startswith('_'):
                continue

            # Format the value, handling NaN and None
            if pd.isna(value):
                display_value = '???'
            else:
                value_str = str(value)
                # Check if value is a URL and convert to clickable link
                if ("www" in value_str and ".com" in value_str) or ("http" in value_str) or ('.ca' in value_str):
                    display_value = f'<a href="{value_str}" target="_blank">{value_str}</a>'
                else:
                    display_value = value_str

            # Create a readable label from column name
            label = col.replace('_', ' ').title()
            popup_parts.append(f"<b>{label}:</b> {display_value}")
        
        # Create HTML for edit button with embedded JavaScript
        gcid = row.get('gcid', 'unknown')
        edit_button = f'''
        <br><br>
        <button onclick="editRow({idx}, '{gcid}')" style="background-color:#4CAF50;color:white;padding:10px;border:none;border-radius:4px;cursor:pointer;margin-right:5px;">Edit Row</button>
        <button onclick="linkExternalData({idx}, '{gcid}')" style="background-color:#2196F3;color:white;padding:10px;border:none;border-radius:4px;cursor:pointer;">Link External Data</button>
        '''

        popup_text = "<br>".join(popup_parts) + edit_button
        
        if row.get('url', 'NOMATCH') == "NOMATCH":
            marker_color = 'red'
        else:
            marker_color = 'blue'

        # Create marker
        marker = folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_text, max_width=400),
            tooltip=row.get('CourseName', 'Golf Course'),
            icon=folium.Icon(color=marker_color, icon='info-sign')
        )
        
        # Store gcid as marker option for hover detection
        marker.options['gcid'] = gcid
        marker.add_to(marker_cluster)
    
    # Build the rowDataMap from unique_locations (keys are unique_locations indexes)
    row_data_map = {str(idx): row.to_dict() for idx, row in unique_locations.iterrows()}
    
    # Build a map of gcid to polygon GeoJSON for JavaScript access
    polygon_geojson_map = {}
    for gcid, feature in geojson_polygons.items():
        polygon_geojson_map[gcid] = feature

    # Load external data for search functionality
    repo_root = Path(__file__).parent.resolve()
    external_csv = repo_root / "data" / "golfLinkData.csv"
    external_data = load_external_data(str(external_csv)) if external_csv.exists() else pd.DataFrame()
    external_json = external_data.to_json(orient='records', default_handler=str) if not external_data.empty else '[]'

    # Make sure source paths in row_data_map are relative (already set in combined df)
    # Inject custom JavaScript for edit functionality
    edit_js = r'''
    <script>
    // Store active polygon layers for cleanup
    let activePolygonLayers = [];
    
    function showCoursePolygon(gcid) {
        // Clear existing polygons
        activePolygonLayers.forEach(layer => {
            window.map.removeLayer(layer);
        });
        activePolygonLayers = [];
        
        // Get polygon data
        const geojsonPolygons = window.polygonGeojsonMap || {};
        if (!geojsonPolygons[gcid]) {
            return;
        }
        
        const feature = geojsonPolygons[gcid];
        
        // Add the polygon to the map with styling
        const geoJsonLayer = L.geoJSON(feature, {
            style: {
                color: '#FF6B6B',
                weight: 3,
                opacity: 0.8,
                fillOpacity: 0.3,
                fillColor: '#FF6B6B'
            },
            onEachFeature: (feature, layer) => {
                activePolygonLayers.push(layer);
            }
        });
        
        geoJsonLayer.addTo(window.map);
        activePolygonLayers.push(geoJsonLayer);
        
        // Optionally fit map to polygon bounds
        if (geoJsonLayer.getBounds) {
            window.map.fitBounds(geoJsonLayer.getBounds(), { padding: [50, 50] });
        }
    }
    
    function hideCoursePolygon() {
        activePolygonLayers.forEach(layer => {
            window.map.removeLayer(layer);
        });
        activePolygonLayers = [];
    }
    
    function editRow(rowIdx, gcid) {
        const rowData = window.rowDataMap[rowIdx];
        let formHtml = '<div style="max-height: 500px; overflow-y: auto;">';
        
        for (const [key, value] of Object.entries(rowData)) {
            // Skip internal source keys
            if (typeof key === 'string' && key.startsWith('_')) continue;

            const displayValue = value === null || value === undefined ? '' : value;

            // Show manually_edited as read-only
            if (key === 'manually_edited') {
                formHtml += `<div style="margin-bottom: 10px;">
                    <label style="font-weight: bold; display: block; margin-bottom: 5px;">${key}:</label>
                    <input type="text" id="edit_${key}" value="${displayValue}" style="width: 100%; padding: 5px; border: 1px solid #ccc; border-radius: 3px;" readonly>
                </div>`;
            } else {
                formHtml += `<div style="margin-bottom: 10px;">
                    <label style="font-weight: bold; display: block; margin-bottom: 5px;">${key}:</label>
                    <input type="text" id="edit_${key}" value="${displayValue}" style="width: 100%; padding: 5px; border: 1px solid #ccc; border-radius: 3px;">
                </div>`;
            }
        }
        
        formHtml += '</div>';
        
        const modal = document.createElement('div');
        modal.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); display: flex; justify-content: center; align-items: center; z-index: 10000;';
        
        const content = document.createElement('div');
        content.style.cssText = 'background: white; padding: 20px; border-radius: 8px; max-width: 600px; max-height: 80vh; overflow-y: auto;';
        
        const title = document.createElement('h2');
        title.textContent = `Edit Row - GCID: ${gcid}`;
        
        const form = document.createElement('div');
        form.innerHTML = formHtml;
        
        const buttonContainer = document.createElement('div');
        buttonContainer.style.cssText = 'margin-top: 20px; display: flex; gap: 10px; justify-content: flex-end;';
        
        const saveBtn = document.createElement('button');
        saveBtn.textContent = 'Save Changes';
        saveBtn.style.cssText = 'background-color: #4CAF50; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer;';
        saveBtn.onclick = () => saveRowChanges(rowIdx, modal);
        
        const closeBtn = document.createElement('button');
        closeBtn.textContent = 'Cancel';
        closeBtn.style.cssText = 'background-color: #f44336; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer;';
        closeBtn.onclick = () => modal.remove();
        
        buttonContainer.appendChild(saveBtn);
        buttonContainer.appendChild(closeBtn);
        
        content.appendChild(title);
        content.appendChild(form);
        content.appendChild(buttonContainer);
        modal.appendChild(content);
        document.body.appendChild(modal);
    }
    
    function saveRowChanges(rowIdx, modal) {
        const rowData = window.rowDataMap[rowIdx];
        const updates = {};

        // Collect edited fields (skip internal source markers)
        for (const key of Object.keys(rowData)) {
            if (key === '_source_file' || key === '_source_index') continue;
            const inputElement = document.getElementById(`edit_${key}`);
            if (inputElement) {
                updates[key] = inputElement.value;
            }
        }

        // Include source file and source index so server can update the correct CSV
        const source_file = rowData['_source_file'];
        const source_index = rowData['_source_index'];

        fetch('/api/update_row', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({rowIdx: rowIdx, updates: updates, source_file: source_file, source_index: source_index})
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                alert('Row saved successfully!');
                modal.remove();
                location.reload();
            } else {
                alert('Error saving row: ' + data.error);
            }
        })
        .catch(error => {
            alert('Error: ' + error);
        });
    }
    
    function linkExternalData(rowIdx, gcid) {
        const rowData = window.rowDataMap[rowIdx];
        const externalData = window.externalData || [];
        
        if (!externalData || externalData.length === 0) {
            alert('No external data available.');
            return;
        }
        
        // Create search modal
        const modal = document.createElement('div');
        modal.style.cssText = 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); display: flex; justify-content: center; align-items: center; z-index: 10000; overflow-y: auto;';
        
        const content = document.createElement('div');
        content.style.cssText = 'background: white; padding: 20px; border-radius: 8px; max-width: 800px; margin: 20px auto;';
        
        const title = document.createElement('h2');
        title.textContent = `Link External Data - Search`;
        content.appendChild(title);
        
        // Search input
        const searchLabel = document.createElement('label');
        searchLabel.textContent = 'Search by course name, city, or state:';
        searchLabel.style.cssText = 'display: block; margin-bottom: 8px; font-weight: bold;';
        content.appendChild(searchLabel);
        
        const searchInput = document.createElement('input');
        searchInput.type = 'text';
        searchInput.placeholder = 'e.g., "Pebble Beach", "San Diego", "CA"';
        searchInput.style.cssText = 'width: 100%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; margin-bottom: 15px; font-size: 14px;';
        content.appendChild(searchInput);
        
        // Results container
        const resultsDiv = document.createElement('div');
        resultsDiv.style.cssText = 'max-height: 400px; overflow-y: auto; border: 1px solid #ccc; padding: 10px; margin-bottom: 15px; border-radius: 4px; background: #f9f9f9;';
        resultsDiv.id = 'searchResults';
        content.appendChild(resultsDiv);
        
        let selectedMatch = null;
        
        // Function to filter and display results
        const performSearch = () => {
            const searchTerm = searchInput.value.toLowerCase().trim();
            resultsDiv.innerHTML = '';
            
            if (!searchTerm) {
                resultsDiv.innerHTML = '<div style="color: #999; text-align: center; padding: 20px;">Enter a search term to find courses</div>';
                selectedMatch = null;
                return;
            }
            
            const matches = externalData.filter(ext => {
                const extName = (ext['CourseName'] || '').toLowerCase();
                const extCity = (ext['City'] || '').toLowerCase();
                const extState = (ext['State'] || '').toLowerCase();
                return extName.includes(searchTerm) || extCity.includes(searchTerm) || extState.includes(searchTerm);
            });
            
            if (matches.length === 0) {
                resultsDiv.innerHTML = `<div style="color: #999; text-align: center; padding: 20px;">No matches found for "${searchTerm}"</div>`;
                selectedMatch = null;
                return;
            }
            
            // Display matches
            // Display matches
            matches.forEach((match, midx) => {
                const matchItem = document.createElement('div');
                matchItem.style.cssText = `padding: 10px; margin-bottom: 10px; border: 2px solid ${selectedMatch === match ? '#4CAF50' : '#ccc'}; border-radius: 4px; cursor: pointer; background: ${selectedMatch === match ? '#f0fff0' : 'white'};`;
                
                const matchInfo = document.createElement('div');
                matchInfo.innerHTML = `
                    <strong>${match['CourseName'] || 'N/A'}</strong><br>
                    City: ${match['City'] || 'N/A'}, State: ${match['State'] || 'N/A'}<br>
                    Holes: ${match['NumHoles'] || 'N/A'} | Par: ${match['Par'] || 'N/A'} | Yardage: ${match['Yardage'] || 'N/A'}
                `;
                
                matchItem.appendChild(matchInfo);
                matchItem.onclick = () => {
                    // Update selection styling
                    Array.from(resultsDiv.children).forEach(child => {
                        child.style.borderColor = '#ccc';
                        child.style.background = 'white';
                    });
                    matchItem.style.borderColor = '#4CAF50';
                    matchItem.style.background = '#f0fff0';
                    selectedMatch = match;
                    updatePreview();
                };
                
                resultsDiv.appendChild(matchItem);
            });
        };
        
        // Show field merge preview
        const previewTitle = document.createElement('h3');
        previewTitle.textContent = 'Fields to Merge (fill missing data)';
        content.appendChild(previewTitle);
        
        const previewDiv = document.createElement('div');
        previewDiv.style.cssText = 'max-height: 250px; overflow-y: auto; background: #f9f9f9; padding: 10px; border-radius: 4px; margin-bottom: 15px; border: 1px solid #ddd;';
        previewDiv.id = 'mergePreview';
        content.appendChild(previewDiv);
        
        // Update preview on selection
        const updatePreview = () => {
            previewDiv.innerHTML = '';
            if (!selectedMatch) {
                previewDiv.innerHTML = '<div style="color: #999;">Select a match to preview fields.</div>';
                return;
            }
            for (const [key, value] of Object.entries(selectedMatch)) {
                if (key.startsWith('_')) continue;
                const currentVal = rowData[key] || '';
                const externalVal = value || '';
                
                // Only show fields where external has data but current doesn't
                if (externalVal && !currentVal) {
                    const fieldDiv = document.createElement('div');
                    fieldDiv.style.cssText = 'margin-bottom: 8px; padding: 8px; background: white; border-left: 3px solid #2196F3; border-radius: 2px;';
                    fieldDiv.innerHTML = `<strong>${key}:</strong> ${externalVal}`;
                    previewDiv.appendChild(fieldDiv);
                }
            }
            if (previewDiv.children.length === 0) {
                previewDiv.innerHTML = '<div style="color: #999;">No missing fields to fill.</div>';
            }
        };
        
        // Set up search listener
        searchInput.oninput = performSearch;
        searchInput.onkeyup = performSearch;
        
        // Initialize with empty state
        updatePreview();
        
        // Button container
        const buttonContainer = document.createElement('div');
        buttonContainer.style.cssText = 'display: flex; gap: 10px; justify-content: flex-end;';
        
        const mergeBtn = document.createElement('button');
        mergeBtn.textContent = 'Merge Data';
        mergeBtn.style.cssText = 'background-color: #4CAF50; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer;';
        mergeBtn.onclick = () => {
            if (!selectedMatch) {
                alert('Please select a match first.');
                return;
            }
            mergeExternalRow(rowIdx, selectedMatch, modal);
        };
        
        const cancelBtn = document.createElement('button');
        cancelBtn.textContent = 'Cancel';
        cancelBtn.style.cssText = 'background-color: #f44336; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer;';
        cancelBtn.onclick = () => modal.remove();
        
        buttonContainer.appendChild(mergeBtn);
        buttonContainer.appendChild(cancelBtn);
        content.appendChild(buttonContainer);
        
        modal.appendChild(content);
        document.body.appendChild(modal);
        
        // Focus search input and trigger initial load of first 10 results
        searchInput.focus();
        selectedMatch = null;
    }
    
    function mergeExternalRow(rowIdx, externalMatch, modal) {
        const rowData = window.rowDataMap[rowIdx];
        const updates = {};
        
        // Merge: fill in missing fields from external data
        for (const [key, value] of Object.entries(externalMatch)) {
            if (key.startsWith('_')) continue;
            const currentVal = rowData[key];
            if (currentVal && value && currentVal == 'NOMATCH') {
                updates[key] = value;
            }
        }
        
        if (Object.keys(updates).length === 0) {
            alert('No new data to merge.');
            return;
        }
        
        // Include source file and source index
        const source_file = rowData['_source_file'];
        const source_index = rowData['_source_index'];
        
        fetch('/api/update_row', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({rowIdx: rowIdx, updates: updates, source_file: source_file, source_index: source_index})
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                alert(`Merged ${Object.keys(updates).length} field(s) successfully!`);
                modal.remove();
                location.reload();
            } else {
                alert('Error merging data: ' + data.error);
            }
        })
        .catch(error => {
            alert('Error: ' + error);
        });
    }
    </script>
    '''
    
    # Save map
    golf_map.save(output_file)

    # Inject the row data and JavaScript into the HTML file
    with open(output_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Store row data as JavaScript object (from unique_locations)
    row_data_script = f'<script>window.rowDataMap = {json.dumps(row_data_map, default=str)};</script>'
    external_data_script = f'<script>window.externalData = {external_json};</script>'
    
    # Store polygon GeoJSON data
    polygon_data_script = f'<script>window.polygonGeojsonMap = {json.dumps(polygon_geojson_map, default=str)};</script>'
    
    # Add hover event listener initialization script and search UI
    hover_script = r'''
    <script>
    // Initialize hover listeners after page loads
    document.addEventListener('DOMContentLoaded', function() {
        // Determine Leaflet map object and store on window.map
        try {
            if (typeof map !== 'undefined' && map) {
                window.map = map;
            } else {
                // find first global that looks like a Leaflet map (has setView & eachLayer)
                for (const k in window) {
                    try {
                        const v = window[k];
                        if (v && typeof v.setView === 'function' && typeof v.eachLayer === 'function') {
                            window.map = v;
                            console.log('Detected Leaflet map in window.' + k);
                            break;
                        }
                    } catch (e) {}
                }
            }
        } catch (e) { console.error('Error finding map object', e); }
        
        // Find all markers and attach hover listeners
        if (window.L && window.L.marker) {
            // For each layer in the map, attach hover events to any markers found (recursively handle clusters/groups)
            function attachToLayer(l) {
                try {
                    if (l instanceof L.Marker) {
                        const gcid = l.options && l.options.gcid ? l.options.gcid : null;
                        if (gcid) {
                            l.on('mouseover', function() { showCoursePolygon(gcid); });
                            l.on('mouseout', function() { hideCoursePolygon(); });
                        } else {
                            // fallback: try to extract gcid from popup HTML
                            l.on('mouseover', function() {
                                try {
                                    const popupHtml = l.getPopup() && l.getPopup().getContent ? l.getPopup().getContent() : '';
                                    const match = popupHtml.match(/editRow\((\d+),\s*'([^']+)'\)/);
                                    if (match) {
                                        showCoursePolygon(match[2]);
                                    }
                                } catch (e) {}
                            });
                            l.on('mouseout', function() { hideCoursePolygon(); });
                        }
                    } else if (l && l._layers) {
                        // LayerGroup or MarkerCluster: iterate inner layers
                        Object.values(l._layers).forEach(function(inner) { attachToLayer(inner); });
                    }
                } catch (e) {}
            }

            map.eachLayer(function(layer) { attachToLayer(layer); });
        }
    });

    // Simple search box UI for jumping to a GCID
    function createGcidSearchBox() {
        const container = document.createElement('div');
        container.style.cssText = 'position: absolute; top: 10px; left: 10px; z-index:10000; background: white; padding: 8px; border-radius: 6px; box-shadow: 0 2px 6px rgba(0,0,0,0.2); font-family: Arial, sans-serif;';
        container.id = 'gcid-search-box';

        const input = document.createElement('input');
        input.type = 'text';
        input.placeholder = 'Enter GCID (e.g. AA00216)';
        input.style.cssText = 'width: 180px; padding: 6px; margin-right: 6px; border: 1px solid #ccc; border-radius: 4px;';
        input.id = 'gcidInput';

        const btn = document.createElement('button');
        btn.textContent = 'Go';
        btn.style.cssText = 'padding: 6px 10px; background:#2196F3; color:white; border:none; border-radius:4px; cursor:pointer;';
        btn.onclick = function() {
            const val = document.getElementById('gcidInput').value.trim();
            if (val) gotoGCID(val);
        };

        container.appendChild(input);
        container.appendChild(btn);
        document.body.appendChild(container);
    }

    function gotoGCID(gcid) {
        try {
            console.log('gotoGCID called for', gcid);
            // Search rowDataMap for matching gcid
            const rows = window.rowDataMap || {};
            let found = null;
            for (const [key, row] of Object.entries(rows)) {
                if (row.gcid && String(row.gcid).toLowerCase() === String(gcid).toLowerCase()) {
                    found = row;
                    break;
                }
            }
            if (!found) {
                alert('GCID not found: ' + gcid);
                return;
            }

            const lat = parseFloat(found.latitude);
            const lon = parseFloat(found.longitude);
            if (isNaN(lat) || isNaN(lon)) {
                alert('No coordinates for GCID: ' + gcid);
                return;
            }

            // Ensure map reference
            if (!window.map) {
                console.warn('window.map not set; attempting to detect map');
                for (const k in window) {
                    try {
                        const v = window[k];
                        if (v && typeof v.setView === 'function' && typeof v.eachLayer === 'function') {
                            window.map = v;
                            console.log('Detected Leaflet map in window.' + k);
                            break;
                        }
                    } catch (e) {}
                }
            }

            // Pan to location and open popup for the marker at this lat/lon
            window.map.setView([lat, lon], Math.max(window.map.getZoom(), 12));

            // Find marker by location (tolerance to floating point)
            let matchedMarker = null;
            window.map.eachLayer(function(layer) {
                try {
                    if (layer instanceof L.Marker) {
                        const pos = layer.getLatLng();
                        if (Math.abs(pos.lat - lat) < 1e-6 && Math.abs(pos.lng - lon) < 1e-6) {
                            matchedMarker = layer;
                        }
                    } else if (layer && layer._layers) {
                        // check internal markers
                        Object.values(layer._layers).forEach(function(inner) {
                            try {
                                if (inner instanceof L.Marker) {
                                    const pos = inner.getLatLng();
                                    if (Math.abs(pos.lat - lat) < 1e-6 && Math.abs(pos.lng - lon) < 1e-6) {
                                        matchedMarker = inner;
                                    }
                                }
                            } catch (e) {}
                        });
                    }
                } catch (e) {}
            });

            if (matchedMarker) {
                matchedMarker.openPopup();
            } else {
                console.warn('No marker found exactly at', lat, lon);
            }

            // Show polygon if available
            showCoursePolygon(gcid);
        } catch (err) {
            console.error('Error in gotoGCID', err);
            alert('Error locating GCID: ' + err);
        }
    }

    // Create the search box once the DOM is ready
    document.addEventListener('DOMContentLoaded', function() { createGcidSearchBox(); });
    </script>
    '''

    # Inject scripts before closing body tag
    html_content = html_content.replace('</body>', f'{row_data_script}\n{external_data_script}\n{polygon_data_script}\n{edit_js}\n{hover_script}\n</body>')

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"Map saved to: {output_file}")

    return golf_map


def main(csv_paths, output_file: str = "golf_courses_map.html", serve=True, port=8000):
    """
    Main function to generate golf course map.
    
    Args:
        csv_path: Path to the CSV file containing golf course data
        output_file: Name of the output HTML file
        serve: Whether to start a local web server (default: True)
        port: Port number for the web server (default: 8000)
    """
    global csv_path_global
    # Allow csv_paths to be a single string or a list
    if isinstance(csv_paths, (list, tuple)):
        input_paths = csv_paths
    else:
        input_paths = [csv_paths]

    csv_path_global = [str(p) for p in input_paths]

    # Load data
    print("Loading golf course data from:")
    for p in input_paths:
        print(f" - {p}")

    # Combine multiple CSVs
    df = load_multiple_csvs(input_paths)
    
    # Get unique locations
    print("\nExtracting unique locations...")
    unique_locations = get_unique_locations(df)
    
    # Create map
    print("\nGenerating map...")

    # Save the HTML one directory above the `images/` folder (i.e., repo root next to `images/`)
    repo_root = Path(__file__).parent.resolve()
    map_file_path = repo_root / output_file
    create_golf_map(df, unique_locations, None, str(map_file_path))

    if serve:
        # Serve from repository root so the HTML can reference the `images/` folder as a sibling
        serve_dir = repo_root
        os.chdir(serve_dir)

        # Start local server
        server_address = ('localhost', port)
        httpd = HTTPServer(server_address, CustomRequestHandler)

        # Run server in a background thread
        server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()

        map_url = f"http://localhost:{port}/{output_file}"
        print(f"\nLocal server started on {map_url}")
        print("The map will now open in your browser.")
        print("Edit features are now enabled! Close the server when done.")

        # Open browser
        webbrowser.open(map_url)

        # Keep server running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nServer stopped.")
            httpd.shutdown()
    else:
        print("\nDone!")


if __name__ == "__main__":
    # Default path to the CSV file
    csv_file_canada = Path(__file__).parent / "data" / "canada" / "Fully_Matched_Golf_Courses.csv"
    csv_file_usa = Path(__file__).parent / "data" / "usa" / "Fully_Matched_Golf_Courses.csv"
    csv_file_mexico = Path(__file__).parent / "data" / "mexico" / "Fully_Matched_Golf_Courses.csv"
    csv_file_world = Path(__file__).parent / "data" / "world" / "combined.csv"

    files = (str(csv_file_canada), str(csv_file_usa), str(csv_file_mexico), str(csv_file_world))
    
    main(files)
