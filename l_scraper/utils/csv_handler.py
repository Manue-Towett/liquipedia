import pandas as pd

from .logger import Logger


class CSVHandler:
    def __init__(self, headers:list, profiles:list, history:list, 
                 achievements:list, file_dir:str) -> None:
        self.logger = Logger("CSVHandler")

        self.headers = headers
        self.profiles = profiles
        self.history = history
        self.achievements = achievements
        self.output_path = file_dir

    def dict_to_dataframe(self) -> pd.DataFrame:
        """Converts dictionary to dataframe"""
        self.logger.info("Converting dictionary to dataframe...")
        
        profiles_df = pd.DataFrame.from_dict(self.profiles)
        column_names = profiles_df.columns.values.tolist()

        profiles_df = profiles_df[
            [column for column in self.headers if column in column_names]
        ]

        history_df = pd.DataFrame.from_dict(self.history)

        achievements_df = pd.DataFrame.from_dict(self.achievements)

        return profiles_df, history_df, achievements_df
    
    def save_to_excel(self) -> None:
        """Saves data to excel file"""
        self.logger.info(f"Done scraping. Saving to >> {self.output_path}")

        profiles, history, achievements = self.dict_to_dataframe()

        profiles.to_excel(self.output_path, sheet_name="profiles", index=False)

        with pd.ExcelWriter(self.output_path, mode="a") as writer:  
            history.to_excel(writer, sheet_name="history", index=False)
            achievements.to_excel(writer, sheet_name="achievements", index=False)

        self.logger.info("Records saved!")