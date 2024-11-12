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
        post = {}

        post_info =  soup.find('table', {'border':'0', 'width':'100%', 'cellpadding':'1', 'cellspacing':'0'})
        if post_info: 
                post['username'] = post_info.find('b').text if post_info.find('b') else ''
                join_date = re.search(r'חבר מתאריך (\d{1,2}\.\d{1,2}\.\d{2,4})', post_info.text)
                post['join_date'] = join_date.group(1) if join_date else ''
                post_count = re.search(r'(\d+) הודעות', post_info.text)
                post['message_count'] = int(post_count.group(1)) if post_count else 0
                rating = re.search(r'(\d+) מדרגים', post_info.text)
                post['rating'] = int(rating.group(1)) if post_count else 0
                content_block = post_info.find('h1', {'class': 'text16b'})  # Adjust class if needed
                post['title'] = content_block.text.strip() if content_block else ''
                content_block = post_info.find('font', {'class': 'text15'})  # Adjust class if needed
                post['content'] = content_block.text.strip() if content_block else ''
                #source = post_info.find('a', href=re.compile(r'passportnews'))
                #post['source'] = source.text.strip() if source else ''
                # Extract date and time
                date_time = post_info.find('font', {'size':'1', 'face':'Arial', 'color':'#000099'})
                if date_time:
                    date = date_time.find_all('font', {'color': '#eeeeee'})[-1].find_next_sibling(text=True).strip()
                    post['date'] = date = date if date else ''
                    post['time'] = date_time.find_next('font', {'color': 'red'}).text.strip()


        # Extract data
        responses = []

# שליפת כל הטבלאות לפי הקריטריונים
        comments_div = soup.find('div', {'id': 'comments_wrap'})


        for table in comments_div.find_all('table', {'width': '100%', 'cellpadding': '3', 'cellspacing': '0'}):
            #if index == 0 or index == 1:
            #    continue  # דלג על הטבלה הראשונה
            response = {}
            
            # Extract user info
            user_info = table.find('font', {'size': '2', 'face': 'Arial', 'color': '#000099'})
            if user_info:
                response['username'] = user_info.find('b').text if user_info.find('b') else ''
                join_date = re.search(r'חבר מתאריך (\d{1,2}\.\d{1,2}\.\d{2,4})', user_info.text)
                response['join_date'] = join_date.group(1) if join_date else ''
                message_count = re.search(r'(\d+) הודעות', user_info.text)
                response['message_count'] = int(message_count.group(1)) if message_count else 0

            # Extract date and time
                date_time = table.find('font', {'size':'1', 'face':'Arial', 'color':'#000099'})
                if date_time:
                    date = date_time.find_all('font', {'color': '#eeeeee'})[-1].find_next_sibling(text=True).strip()
                    response['date'] = date = date if date else ''
                    response['time'] = date_time.find_next('font', {'color': 'red'}).text.strip()


            # Extract content
            content = table.find('font', {'class': 'text16b'})
            if content:
                response['title'] = content.text.strip()
            content_block = table.find('font', {'class': 'text15'})  # Adjust class if needed
            response['content'] = content_block.text.strip() if content_block else ''

            # Extract reply to info
            reply_to = table.find('a', {'href': re.compile(r'#\d+$')})
            if reply_to:
                reply_to_text = reply_to.text.split()[-1]
                if reply_to_text.isdigit():
                    response['reply_to'] = int(reply_to_text)
                else:
                    response['reply_to'] = reply_to_text  # Or handle it in a way that makes sense for your application
            
            responses.append(response)

        post['responses'] = responses
        # Generate output filename
        output_filename = os.path.splitext(os.path.basename(file_path))[0] + '.json'
        output_path = os.path.join('D:\\json_files\\nisuy', output_filename)
        
        yield (output_path, json.dumps({'post': post}, ensure_ascii=False))

def write_json_file(element):
    output_path, json_content = element
    #encoding = chardet.detect(element)['encoding']
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(json_content)

def run():
    with beam.Pipeline() as pipeline:
        (pipeline
         | 'Create file list' >> beam.Create(os.listdir('D:\\files_all\\nisuy'))
         | 'Add full path' >> beam.Map(lambda x: os.path.join('D:\\files_all\\nisuy', x))
         | 'Parse HTML' >> beam.ParDo(HTMLParserDoFn())
         | 'Write JSON' >> beam.Map(write_json_file)
        )

if __name__ == '__main__':
    run()