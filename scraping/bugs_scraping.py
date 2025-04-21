import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import List, Dict, Optional, Union
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BugsMusicScraper:
    """Class to handle scraping operations for Bugs Music website."""

    BASE_URL = "https://music.bugs.co.kr"

    def __init__(self, request_delay: float = 0.5):
        """
        Initialize the scraper with delay between requests to avoid throttling.

        Args:
            request_delay: Time in seconds to wait between requests
        """
        self.request_delay = request_delay

    def _get_soup(self, url: str) -> BeautifulSoup:
        """
        Make a request to the URL and return a BeautifulSoup object.

        Args:
            url: The URL to request

        Returns:
            BeautifulSoup object for parsing
        """
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(f"Failed to get {url}, status code: {response.status_code}")
            response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        time.sleep(self.request_delay)  # Add delay between requests
        return soup

    def get_artist_albums(self, artist_id: str) -> pd.DataFrame:
        """
        Get all albums for a given artist ID.

        Args:
            artist_id: The artist ID from Bugs music

        Returns:
            DataFrame containing album information
        """
        url = f"{self.BASE_URL}/artist/{artist_id}/albums"
        soup = self._get_soup(url)

        album_data = {
            'album_id': [],
            'album_title': [],
            'artist_name': [],
            'release_date': [],
            'type': []
        }

        for album_info in soup.find_all("figure", attrs={'class': 'albumInfo'}):
            album_data['album_id'].append(album_info['albumid'])
            album_data['album_title'].append(album_info.find("div", attrs={'class': 'albumTitle'}).text.strip('\n'))
            album_data['artist_name'].append(album_info.find('p', attrs={'class': 'artist'}).text.strip('\n'))
            album_data['release_date'].append(album_info.find('time'))
            album_data['type'].append(album_info.find('span', attrs={'class': 'albumType'}))

        df = pd.DataFrame(album_data)
        df['artist_id'] = artist_id

        return df

    def get_album_details(self, album_id: str) -> Optional[pd.DataFrame]:
        """
        Get detailed information about an album.

        Args:
            album_id: The album ID from Bugs music

        Returns:
            DataFrame containing album details or None if scraping fails
        """
        try:
            url = f"{self.BASE_URL}/album/{album_id}"
            soup = self._get_soup(url)

            # Get thumbnail
            thumbnail = soup.find('li', attrs={'class': 'big'}).a.img['src']

            # Get album info from table
            temp_list = []
            for table in soup.find_all('table', attrs={'class': 'info'}):
                for cell in table.find('tbody').find_all('td'):
                    temp_list.append(cell.text.strip())

            df = pd.DataFrame({
                "album_id": album_id,
                "genre": temp_list[2],
                "style": temp_list[3],
                "distributor": temp_list[5],
                "agency": temp_list[6],
                "total_duration": temp_list[7],
                "thumbnail": thumbnail
            }, index=[0])

            return df

        except Exception as e:
            logger.error(f"Failed to get details for album {album_id}: {str(e)}")
            return None

    def get_album_tracks(self, album_id: str) -> Optional[pd.DataFrame]:
        """
        Get all tracks for a given album ID.

        Args:
            album_id: The album ID from Bugs music

        Returns:
            DataFrame containing track information or None if scraping fails
        """
        try:
            url = f"{self.BASE_URL}/album/{album_id}"
            soup = self._get_soup(url)

            track_ids = []
            track_titles = []

            for elem in soup.find_all('p', attrs={'class': 'title'}):
                # Extract track ID from onclick attribute
                track_id = elem.a['onclick'].lstrip("bugs.wiselog.area('list_tr_09_ab');bugs.music.listen('").split("'")[0]
                track_ids.append(track_id)
                track_titles.append(elem.text.replace('\n', ''))

            df = pd.DataFrame({
                'album_id': album_id,
                'track_id': track_ids,
                'track_title': track_titles
            })

            return df

        except Exception as e:
            logger.error(f"Failed to get tracks for album {album_id}: {str(e)}")
            return None

    def get_track_lyrics(self, track_id: str) -> Optional[pd.DataFrame]:
        """
        Get lyrics and duration for a given track ID.

        Args:
            track_id: The track ID from Bugs music

        Returns:
            DataFrame containing track details or None if scraping fails
        """
        try:
            url = f"{self.BASE_URL}/track/{track_id}"
            soup = self._get_soup(url)

            duration = soup.find('time').text
            # Get lyrics and replace line breaks with spaces
            lyrics = soup.find('div', {'class': 'lyricsContainer'}).xmp.text.replace('\r\n', ' ')

            df = pd.DataFrame({
                'track_id': track_id,
                'duration': duration,
                'lyrics': lyrics
            }, index=[0])

            return df

        except Exception as e:
            logger.error(f"Failed to get lyrics for track {track_id}: {str(e)}")
            return None


def main():
    """Main execution function."""
    # Load artist IDs from Excel
    try:
        list_df = pd.read_excel('../data/artists/kpop_girlgroup_list.xlsx', sheet_name='group')
        artist_id_list = list_df.artist_id.tolist()
    except Exception as e:
        logger.error(f"Failed to load artist list: {str(e)}")
        return

    # Initialize scraper
    scraper = BugsMusicScraper(request_delay=0.5)

    # Step 1: Get all albums for each artist
    logger.info("Collecting album information for all artists...")
    artist_album_dfs = []

    for idx, artist_id in enumerate(artist_id_list):
        logger.info(f"Processing artist {idx+1}/{len(artist_id_list)}: {artist_id}")
        artist_df = scraper.get_artist_albums(artist_id)
        artist_album_dfs.append(artist_df)

    # Combine all artist albums
    album_df_total = pd.concat(artist_album_dfs)

    # Clean the album dataset
    album_clean = album_df_total[~album_df_total['album_title'].str.contains('권리없는')]
    album_clean = album_clean[~album_clean['album_title'].str.contains(' OST')]
    album_clean = album_clean[~album_clean['album_title'].str.contains('일본')]
    album_clean = album_clean[~album_clean['artist_name'].str.contains('Various Artists')]
    album_clean = album_clean.reset_index(drop=True)

    # Step 2: Get album details for each album
    logger.info(f"Getting details for {len(album_clean)} albums...")
    album_details = []

    for idx, album_id in enumerate(album_clean.album_id.tolist()):
        if idx % 10 == 0:
            logger.info(f"Processing album details {idx}/{len(album_clean)}")

        album_detail = scraper.get_album_details(album_id)
        if album_detail is not None:
            album_details.append(album_detail)

    album_detail_df = pd.concat(album_details).reset_index(drop=True)

    # Merge album information with details
    album_final = album_clean.merge(album_detail_df, on='album_id', how='left')\
        .drop_duplicates(subset=['album_id', 'album_title'])\
        .reset_index(drop=True)

    # Step 3: Get tracks for each album
    logger.info(f"Getting tracks for {len(album_final)} albums...")
    album_tracks = []

    for idx, album_id in enumerate(album_final.album_id.tolist()):
        if idx % 10 == 0:
            logger.info(f"Processing album tracks {idx}/{len(album_final)}")

        tracks = scraper.get_album_tracks(album_id)
        if tracks is not None:
            album_tracks.append(tracks)

    album_track_df = pd.concat(album_tracks)

    # Step 4: Get lyrics for each track
    logger.info(f"Getting lyrics for {len(album_track_df)} tracks...")
    lyrics_dfs = []

    for idx, track_id in enumerate(album_track_df.track_id.tolist()):
        if idx % 20 == 0:
            logger.info(f"Processing lyrics {idx}/{len(album_track_df)}")

        lyrics = scraper.get_track_lyrics(track_id)
        if lyrics is not None:
            lyrics_dfs.append(lyrics)

    # Combine all lyrics
    lyrics_df = pd.concat(lyrics_dfs).reset_index(drop=True)

    # Save outputs to CSV
    album_final.to_csv('kpop_girl_group_albums.csv', index=False)
    album_track_df.to_csv('kpop_girl_group_tracks.csv', index=False)
    lyrics_df.to_csv('kpop_girl_group_lyrics.csv', index=False)

    logger.info("Scraping completed successfully!")


if __name__ == '__main__':
    main()
