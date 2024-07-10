import apache_beam as beam
import aiohttp
import asyncio
from bs4 import BeautifulSoup, SoupStrainer
import json
import re
import os
import chardet

class HTMLParserDoFn(beam.DoFn):
    def process(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract data
        posts = []

        for table in soup.find_all('table', {'width': '100%', 'cellpadding': '3', 'cellspacing': '0'}):
            post = {}
            
            # Extract user info
            user_info = table.find('font', {'size': '2', 'face': 'Arial', 'color': '#000099'})
            if user_info:
                post['username'] = user_info.find('b').text if user_info.find('b') else ''
                join_date = re.search(r'חבר מתאריך (\d{1,2}\.\d{1,2}\.\d{2,4})', user_info.text)
                post['join_date'] = join_date.group(1) if join_date else ''
                post_count = re.search(r'(\d+) הודעות', user_info.text)
                post['post_count'] = int(post_count.group(1)) if post_count else 0

            # Extract date and time
            date_time = table.find('font', {'color': 'black'})
            if date_time:
                post['date'] = date_time.find_next('font', {'color': '#eeeeee'}).find_next(text=True).strip()
                post['time'] = date_time.find_next('font', {'color': 'red'}).text.strip()

            # Extract content
            content = table.find('font', {'class': 'text16b'})
            if content:
                post['content'] = content.text.strip()

            # Extract reply to info
            reply_to = table.find('a', {'href': re.compile(r'#\d+$')})
            if reply_to:
                reply_to_text = reply_to.text.split()[-1]
                if reply_to_text.isdigit():
                    post['reply_to'] = int(reply_to_text)
                else:
                    post['reply_to'] = reply_to_text  # Or handle it in a way that makes sense for your application
            
            posts.append(post)


        # Generate output filename
        output_filename = os.path.splitext(os.path.basename(file_path))[0] + '.json'
        output_path = os.path.join('json_files', output_filename)
        
        yield (output_path, json.dumps({'posts': posts}, ensure_ascii=False))

def write_json_file(element):
    output_path, json_content = element
    #encoding = chardet.detect(element)['encoding']
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(json_content)

def run():
    with beam.Pipeline() as pipeline:
        (pipeline
         | 'Create file list' >> beam.Create(os.listdir('html_files'))
         | 'Add full path' >> beam.Map(lambda x: os.path.join('html_files', x))
         | 'Parse HTML' >> beam.ParDo(HTMLParserDoFn())
         | 'Write JSON' >> beam.Map(write_json_file)
        )

if __name__ == '__main__':
    run()