# Author : Satish Gaikwad
# Purpose: This program is used to scrape the data from a webpage and to save the scraped data into excel sheet.
#          Its also saves the webpages and it's sub-pages into .html files and zip it.
# Version: 1.0
# Date: 31-JUL-2024
# Requirement: https://github.com/satishrg001/web_scraping/etl_task.docx


# Import required python modules

import httplib2
from bs4 import BeautifulSoup, SoupStrainer
import re
from pywebcopy import save_webpage
import logging
import requests
from socket import timeout
from urllib.error import HTTPError, URLError
import datetime
import shutil
import pandas as pd

# Configure logging
current_date_time = datetime.datetime.now().strftime('%H_%M_%S_%d_%m_%Y')
logfile_path = "D:/exercise/"
logging.basicConfig(filename=logfile_path + "scraping_" + current_date_time + ".log", filemode="a",
                    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s", datefmt="%H:%M:%S",
                    level=logging.DEBUG)

logging.info("Scraping Process Started....")

# Define variables
final_url = ""
prj_folder = "D:/exercise/"
prj_name = "hockey_stats_webpages/"
html_site_folder = "www.scrapethissite.com/pages/forms/"
p_num_prefix = "page_num_"
file_extension = ".html"
html_path = ""
p_num = ""
url = 'https://www.scrapethissite.com/pages/forms/'
df = pd.DataFrame()
root_dir = "D:\exercise\hockey_stats_webpages"
archive_path = "D:\exercise\html_webpages_" + current_date_time
excel_file_name = "D:\exercise\hockey_processed_data" + current_date_time + ".xlsx"

# Check url is accessible and get response
try:
    status, response = httplib2.Http().request(url)
except HTTPError as error:
    logging.error('HTTP Error: %s\nURL: %s', error, url)
except URLError as error:
    if isinstance(error.reason, timeout):
        logging.error('Timeout Error: %s\nURL: %s', error, url)
    else:
        logging.error('URL Error:  %s\nURL: %s', error, url)
else:
    logging.info('Url Access successful.')

# Save all the webpages including sub-pages for the url
try:
    for link in BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('a')):
        if link.has_attr('href'):
            text = link.get('href')
            # Get html content having page_num text
            if re.search("page_num", text):
                # Get the starting position of ?page_num
                page_num = text.rfind("?page_num")

                # Extract page_number
                page_num_suffix = text[page_num:]

                # Extract page_number
                p_num_position = text.rfind("=")
                p_num = text[p_num_position + 1:]

                # Prepare final url with page number
                final_url = url + page_num_suffix

                # Save Webpage to html file
                save_webpage(
                    url=final_url,
                    project_folder=prj_folder,
                    project_name=prj_name,
                    bypass_robots=True,
                    debug=False,
                    open_in_browser=False,
                    delay=None,
                    threaded=True
                )
                logging.info(final_url)

                # Load the downloaded HTML file to extract data table from html webpage
                html_path = prj_folder + prj_name + html_site_folder + p_num_prefix + p_num + file_extension
                with open(html_path, 'r', encoding='utf-8') as file:
                    soup = BeautifulSoup(file, 'html.parser')

                # Find the table in HTML page data
                table = soup.find('table')

                # Read the HTML table into a Pandas DataFrame
                df = pd.concat([df, pd.read_html(str(table))[0]], ignore_index=True)

except requests.exceptions.ConnectionError as e:
    logging.info("Connection timeout. URL->%s, Error->%s", final_url, e)
    pass
except Exception as err:
    logging.error(err)

# Sort data based on Year column in Ascending order
df = df.sort_values('Year')

# Filter the data
df = df[(df['Year'] >= 1990) & (df['Year'] <= 2011)]

# Save filtered data into Excel file for NHL Stats 1990-2011
df.to_excel(excel_file_name, index=False, sheet_name="NHL Stats 1990-2011")

df2 = df.groupby(['Year', 'Team Name'])['Wins'].sum()
df3 = df.groupby(['Year', 'Team Name'])['Losses'].sum()

df_concat = pd.concat([df2, df3], axis=1)

# Save wins_losses data into excel file
df_concat.to_excel("D:/exercise/wins_losses.xlsx", sheet_name="Winner and Loser per Year")

logging.info("Zipping html webpages into a zip file")

# Zip downloaded html webpages
shutil.make_archive(archive_path, format='zip', root_dir=root_dir)
logging.info("Zipped...")

logging.info("Scraping Process Completed.")
