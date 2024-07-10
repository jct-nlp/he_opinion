import apache_beam as beam
import aiohttp
import asyncio
from bs4 import BeautifulSoup, SoupStrainer
import chardet

DISALLOWED_TAGS = set(['script'])
#מעבר על רשימת לינקים
#רישום הקבצים שנשמרו בהצלחה
#בדיקה שלא שמרנו כבר את הקובץ

def scrape(argv,num):
    with beam.Pipeline(argv=argv) as p:
        links = p | beam.Create(['https://rotter.net/forum/scoops1/xxx.shtml'.replace('xxx', str(num))])
        
        # Fetch and save HTML content
        html_contents = links | beam.ParDo(FetchAndSaveHTML())
        
        # Further processing can be done here with html_contents

class FetchAndSaveHTML(beam.DoFn):
    def process(self, url):
        html_content = asyncio.run(self.fetch_html(url))
        filename = f"html_files\html_files.{url.replace('://', '_').replace('/', '_')}.html"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Saved HTML content to {filename}")
        
        # Parse the HTML content with BeautifulSoup and extract text
        soup = BeautifulSoup(html_content, 'html.parser')
        for t in soup.find_all(text=True):
            if t.parent.name not in DISALLOWED_TAGS:
                yield t.strip()

    async def fetch_html(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    encoding = chardet.detect(content)['encoding']
                    if not encoding:
                        encoding = 'utf-8'  # fallback to utf-8 if encoding detection fails
                    return content.decode(encoding, errors='replace')
                else:
                    print(f"Failed to retrieve {url}. Status code: {response.status}")
                    return ""

if __name__ == '__main__':
    import sys
    for num in range(855771, 855871):
        scrape(sys.argv,num)