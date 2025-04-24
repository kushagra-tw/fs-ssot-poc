import pandas as pd
import recordlinkage

data1 = {
    'id': [1, 2, 3, 4],
    'name': ['John Smith', 'Jane Doe', 'Robert Brown', 'Alice Johnson'],
    'age': [30, 25, 40, 35]
}
df1 = pd.DataFrame(data1)

data2 = {
    'record_id': ['A', 'B', 'C', 'D'],
    'name': ['Jon Smith', 'J. Doe', 'Bob Brown', 'Alice Jonson'],
    'city': ['New York', 'London', 'Paris', 'Tokyo']
}
df2 = pd.DataFrame(data2)

indexer = recordlinkage.Index()
indexer.block('name')
candidate_links = indexer.index(df1, df2)

compare_cl = recordlinkage.Compare()
compare_cl.string('name', 'name', threshold=0.5, label='name_similarity')
features = compare_cl.compute(candidate_links, df1, df2)

matches = features[features['name_similarity'] >= 0.5]

matched_indices = list(matches.index)
matched_df1_indices = [idx[0] for idx in matched_indices]
matched_df2_indices = [idx[1] for idx in matched_indices]

matched_df1 = df1.loc[matched_df1_indices]
matched_df2 = df2.loc[matched_df2_indices]

matched_df1['record_id'] = matched_df2['record_id'].values

print("Matched DataFrame 1:")
print(matched_df1)

print("\nMatched DataFrame 2:")
print(matched_df2)