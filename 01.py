from datetime import datetime
import os
import pandas as pd
import numpy as np
from glob import glob
import re

# Set directories
wd_input = "Roxas_files"
wd_output = "Roxas_tree_series"

os.chdir(wd_input)
topfolder = os.getcwd()

# List cell and ring files
cell_files = glob(os.path.join(
    topfolder, '**', 'Output_Cells.txt'), recursive=True)
ring_files = glob(os.path.join(
    topfolder, '**', 'Output_Rings.txt'), recursive=True)

len_files = len(cell_files)

metadata = pd.DataFrame({
    'tree': [np.nan] * len_files,
    'slide': [np.nan] * len_files,
    'image': [np.nan] * len_files,
    'imagecode': [np.nan] * len_files,
    'cell.file': cell_files,
    'ring.file': ring_files
})


def extract_code(filepath, pattern):
    return re.sub(pattern, '', os.path.basename(filepath))


for i in range(len_files):
    cell_code = extract_code(metadata['cell.file'][i], "_Output_Cells.txt")
    ring_code = extract_code(metadata['ring.file'][i], "_Output_Rings.txt")

    if cell_code != ring_code:
        raise ValueError(
            f"Cell and ring files are not consistent at line {i + 1}!")


def extract_details(filepath):
    parts = re.sub('_Output_Cells.txt', '',
                   os.path.basename(filepath)).split('_')
    return parts[0], parts[1], parts[2], parts[3], '_'.join(parts[:3]), '_'.join(parts)


for i in range(len_files):
    plot, tree, slide, image, treecode, imagecode = extract_details(
        metadata['cell.file'][i])
    metadata.loc[i, ['plot', 'tree', 'slide', 'image', 'treecode',
                     'imagecode']] = plot, tree, slide, image, treecode, imagecode

metadata['treecode'] = pd.Categorical(metadata['treecode'])

metadata.dropna(subset=['treecode'], inplace=True)

os.chdir(wd_output)

start_time = datetime.now()

for treecode in metadata['treecode'].unique():
    print(f"Processing wood piece: {treecode}")

    mdf = metadata[metadata['treecode'] == treecode]
    numfiles = len(mdf)

    cells_combined = pd.DataFrame()
    rings_combined = pd.DataFrame()

    for f in range(numfiles):
        print(f"Processing file: {mdf.iloc[f]['cell.file']}")

        cells = pd.read_csv(mdf.iloc[f]['cell.file'],
                            sep='\t', na_values=["NA", ""])
        rings = pd.read_csv(mdf.iloc[f]['ring.file'],
                            sep='\t', na_values=["NA", ""])

        # Rename columns for compatibility
        rename_cols = {'BEND': 'TB2', 'CRI': 'TB2', 'CA': 'LA'}
        cells.rename(columns=rename_cols, inplace=True)
        rings = rings[['ID', 'YEAR', 'MRW']]

        if f == 0:
            cells_combined = cells
            rings_combined = rings
        else:
            # Check for overlapping years and merge dataframes
            overlap_years = set(cells['YEAR']).intersection(rings['YEAR'])
            for year in overlap_years:
                cell_count_new = len(cells[cells['YEAR'] == year])
                cell_count_combined = len(
                    cells_combined[cells_combined['YEAR'] == year])

                if cell_count_new > cell_count_combined:
                    cells_combined = cells_combined[cells_combined['YEAR'] != year]
                    rings_combined = rings_combined[rings_combined['YEAR'] != year]

                    cells_combined = pd.concat(
                        [cells_combined, cells[cells['YEAR'] == year]])
                    rings_combined = pd.concat(
                        [rings_combined, rings[rings['YEAR'] == year]])

            non_overlap_cells = cells[~cells['YEAR'].isin(overlap_years)]
            non_overlap_rings = rings[~rings['YEAR'].isin(overlap_years)]

            cells_combined = pd.concat([cells_combined, non_overlap_cells])
            rings_combined = pd.concat([rings_combined, non_overlap_rings])

        # Order by YEAR
        cells_combined.sort_values(by=['YEAR', 'RRADDISTR'], inplace=True)
        rings_combined.sort_values(by='YEAR', inplace=True)

        # Additional data manipulations
        # ...

    # Write to output files
    cells_combined.to_csv(f"{treecode}_Output_Cells.txt", index=False)
    rings_combined.to_csv(f"{treecode}_Output_Rings.txt", index=False)

print(f"Time taken: {datetime.now() - start_time}")
