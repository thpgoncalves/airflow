import os
import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/justmarkham/pandas-videos/master/data/drinks.csv"

def download_and_transform(output_path: str) -> None:
    """
    Download the csv file, transform and save in the output_path
    """
    print("Starting data download...")
    df = pd.read_csv(DATA_URL)

    df_grouped = df.groupby(by="continent", dropna=True)[["beer_servings", "wine_servings", "spirit_servings"]].sum()
    df_grouped.reset_index(inplace=True)

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    df_grouped.to_csv(output_path, index=False)
    print(f"File transformed and saved on: {output_path}")
