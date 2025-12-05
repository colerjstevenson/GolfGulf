import pycancensus as pc
import os

os.environ['CANCENSUS_API_KEY'] = 'CensusMapper_d918ab7e2b0cb08ac7b24a3990a6cb93'

# Search for various demographic vectors
search_terms = [
    ('median age', 'Age'),
    ('population', 'Population'),
    ('median income', 'Income'),
    ('employment', 'Employment'),
    ('education', 'Education'),
    ('housing', 'Housing'),
    ('immigrant', 'Immigration'),
    ('visible minority', 'Race/Ethnicity'),
]

print("Key vectors in CA21 dataset:\n")
for search_term, label in search_terms:
    try:
        vectors_df = pc.search_census_vectors(search_term, dataset='CA21')
        if len(vectors_df) > 0:
            print(f"{label}:")
            for idx, row in vectors_df.head(3).iterrows():
                print(f"  {row['vector']}: {row['label'][:60]}")
            print()
    except Exception as e:
        print(f"Error searching for '{search_term}': {e}\n")
