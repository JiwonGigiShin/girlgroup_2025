import pandas as pd
import logging
import os
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KpopDataCleaner:
    """Class for cleaning and processing scraped K-pop data."""

    def __init__(self,
                album_file: str = 'data/albums.csv',
                track_file: str = 'data/tracks.csv',
                lyrics_file: str = 'data/lyrics.csv',
                output_dir: str = 'data'):
        """
        Initialize the data cleaner.

        Args:
            album_file: Path to the album CSV file
            track_file: Path to the track CSV file
            lyrics_file: Path to the lyrics CSV file
            output_dir: Directory to save output files
        """
        self.album_file = album_file
        self.track_file = track_file
        self.lyrics_file = lyrics_file
        self.output_dir = output_dir

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

    def load_data(self) -> tuple:
        """
        Load the data from CSV files.

        Returns:
            Tuple of DataFrames (album_df, track_df, lyrics_df)
        """
        logger.info("Loading data from CSV files...")

        try:
            album_df = pd.read_csv(self.album_file)
            track_df = pd.read_csv(self.track_file)
            lyrics_df = pd.read_csv(self.lyrics_file)

            logger.info(f"Loaded {len(album_df)} albums, {len(track_df)} tracks, and {len(lyrics_df)} lyrics entries")
            return album_df, track_df, lyrics_df

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise

    def clean_track_ids(self, track_df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean track IDs and handle login layer errors.

        Args:
            track_df: DataFrame containing track information

        Returns:
            Cleaned DataFrame
        """
        # Check for login layer errors in track IDs
        login_layer_rows = track_df[track_df['track_id'].str.contains('howLoginLayer', na=False)]
        if not login_layer_rows.empty:
            logger.warning(f"Found {len(login_layer_rows)} tracks with login layer errors:")
            logger.warning(f"Erroneous tracks: {login_layer_rows['track_title'].tolist()}")
            # Remove these rows
            track_df = track_df[~track_df['track_id'].str.contains('howLoginLayer', na=False)]

        # Convert track_id to numeric if possible
        try:
            track_df['track_id'] = pd.to_numeric(track_df['track_id'], errors='coerce')
            # Drop rows with non-numeric track_ids
            track_df = track_df.dropna(subset=['track_id'])
            track_df['track_id'] = track_df['track_id'].astype('int64')
            logger.info(f"Successfully converted track_ids to integers. {len(track_df)} valid tracks remain.")
        except Exception as e:
            logger.error(f"Error converting track_ids to integers: {str(e)}")
            # If conversion fails, keep as strings but clean them
            track_df['track_id'] = track_df['track_id'].astype(str).str.strip()
            logger.info("Keeping track_ids as strings after cleaning.")

        return track_df

    def clean_html_tags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean HTML tags from various fields.

        Args:
            df: DataFrame containing HTML tag contaminated fields

        Returns:
            Cleaned DataFrame
        """
        logger.info("Cleaning HTML tags from fields...")

        # Clean release_date
        if 'release_date' in df.columns:
            df['release_date'] = df['release_date'].astype(str)
            df['release_date'] = df['release_date'].str.replace(r'<time.*?>(.*?)</time>', r'\1', regex=True)
            # Convert to datetime
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

        # Clean album type
        if 'type' in df.columns:
            df['type'] = df['type'].astype(str)
            df['type'] = df['type'].str.replace(r'<span.*?>(.*?)</span>', r'\1', regex=True)

        # Clean artist name
        if 'artist_name' in df.columns:
            df['artist_name_clean'] = df['artist_name'].astype(str).str.split('\n\n\r\n').str[0]

        # Clean text fields
        for field in ['genre', 'style', 'track_title', 'lyrics']:
            if field in df.columns:
                df[field] = df[field].astype(str).str.replace('\r\n', ' ')
                df[field] = df[field].str.strip()

        return df

    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out unwanted data based on patterns.

        Args:
            df: DataFrame to filter

        Returns:
            Filtered DataFrame
        """
        logger.info(f"Starting filtering process with {len(df)} rows...")

        # Filter out Various Artists
        if 'artist_name' in df.columns:
            df = df[df['artist_name'] != 'Various Artists']
            logger.info(f"After filtering Various Artists: {len(df)} rows")

        # Filter out empty lyrics
        if 'lyrics' in df.columns:
            before_count = len(df)
            df = df[df['lyrics'].str.strip() != '']
            df = df[df['lyrics'] != 'nan']
            logger.info(f"Removed {before_count - len(df)} rows with empty lyrics")

        # Filter by album and track title patterns
        if 'album_title' in df.columns:
            album_patterns = 'remix|japan|일본|inst|repackage|chinese| ver.'
            before_count = len(df)
            df = df[~df['album_title'].str.contains(album_patterns, case=False, na=False)]
            logger.info(f"Removed {before_count - len(df)} rows with unwanted album titles")

        if 'track_title' in df.columns:
            song_patterns = '권리없는|remix|japan|chinese| inst | ver.'
            before_count = len(df)
            df = df[~df['track_title'].str.contains(song_patterns, case=False, na=False)]
            logger.info(f"Removed {before_count - len(df)} rows with unwanted track titles")

        return df

    def process_data(self) -> Optional[pd.DataFrame]:
        """
        Process all data, combining steps of loading, cleaning, and filtering.

        Returns:
            Final processed DataFrame
        """
        try:
            # Load data
            album_df, track_df, lyrics_df = self.load_data()

            # Clean track IDs
            track_df = self.clean_track_ids(track_df)

            # Merge datasets
            logger.info("Merging datasets...")
            df_total = album_df.merge(track_df, how='left', on='album_id')

            # Handle possible track_id type mismatch
            if df_total['track_id'].dtype != lyrics_df['track_id'].dtype:
                logger.info(f"Converting track_id types for merge: {df_total['track_id'].dtype} vs {lyrics_df['track_id'].dtype}")
                # Try to make types match
                if pd.api.types.is_numeric_dtype(df_total['track_id']):
                    lyrics_df['track_id'] = pd.to_numeric(lyrics_df['track_id'], errors='coerce')
                else:
                    df_total['track_id'] = df_total['track_id'].astype(str)
                    lyrics_df['track_id'] = lyrics_df['track_id'].astype(str)

            # Merge with lyrics
            df_total = df_total.merge(lyrics_df, how='left', on='track_id')
            logger.info(f"After merging, total dataset has {len(df_total)} rows")

            # Clean HTML tags
            df_total = self.clean_html_tags(df_total)

            # Apply filters
            df_total = self.filter_data(df_total)

            # Save output
            output_path = os.path.join(self.output_dir, 'kpop_girl_group_data_clean.csv')
            df_total.to_csv(output_path, index=False)
            logger.info(f"Successfully saved cleaned data to {output_path}")

            return df_total

        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            return None


def main():
    """Main execution function."""
    cleaner = KpopDataCleaner()
    df = cleaner.process_data()

    if df is not None:
        logger.info(f"Data cleaning completed successfully! Final dataset has {len(df)} rows")
        # Display some statistics
        logger.info(f"Number of unique albums: {df['album_id'].nunique()}")
        logger.info(f"Number of unique tracks: {df['track_id'].nunique()}")
        logger.info(f"Number of unique artists: {df['artist_name_clean'].nunique()}")
    else:
        logger.error("Data cleaning failed!")


if __name__ == '__main__':
    main()
