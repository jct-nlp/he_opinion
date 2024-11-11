import apache_beam as beam
from bs4 import BeautifulSoup
import json
import re
import os

# Initialize global counters
total_articles = 0
total_comments = 0

class HTMLParserDoFn(beam.DoFn):
    def process(self, file_path):
        global total_articles
        global total_comments

        # Attempt to read the file with windows-1255 encoding, fallback to utf-8 with errors replaced
        try:
            with open(file_path, 'r', encoding='windows-1255') as file:
                html_content = file.read()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                html_content = file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract data
        posts = []
        article_count = 0
        comment_count = 0

        for table in soup.find_all('table', {'width': '100%', 'cellpadding': '3', 'cellspacing': '0'}):
            post = {}
            
            # Extract user info (this helps identify comments)
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
                # Extract time
                time_tag = date_time.find_next('font', {'color': 'red'})
                if time_tag:
                    post['time'] = time_tag.text.strip()
                    
                    # Extract date
                    date_tag = time_tag.find_next_sibling(text=True)
                    if date_tag and date_tag.strip():
                        post['date'] = date_tag.strip()
                    else:
                        date_tag = time_tag.find_next('font', {'color': '#eeeeee', 'size': '1'})
                        if date_tag and date_tag.next_sibling:
                            post['date'] = date_tag.next_sibling.strip()
            
            # Extract title (this identifies an article)
            article_title = table.find('h1', {'class': 'text16b'})
            if article_title:
                post['article_title'] = article_title.text.strip()
                article_count += 1  # Increment article count if an article title is found
            
            # Identify and count comments
            if not article_title and user_info:
                comment_count += 1  # Increment comment count if it's a comment

            # Extract content
            content = table.find('font', {'class': 'text15'})
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

        # Update global counters
        total_articles += article_count
        total_comments += comment_count

        # Create the JSON data with summary
        json_data = {
            'article_count': article_count,
            'comment_count': comment_count,
            'posts': posts
        }

        # Yield the output for the JSON creation
        output_filename = os.path.splitext(os.path.basename(file_path))[0] + '.json'
        output_path = os.path.join('json_files', output_filename)
        yield (output_path, json.dumps(json_data, ensure_ascii=False, indent=4))

def write_json_file(element):
    output_path, json_content = element
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8', errors='replace') as f:
        # Ensure all characters are safe for utf-8
        json_content = json.dumps(json.loads(json_content), ensure_ascii=False)
        f.write(json_content)

def write_total_counts():
    global total_articles
    global total_comments

    count_file_path = 'total_counts.txt'
    with open(count_file_path, 'w', encoding='utf-8') as count_file:
        count_file.write(f"Total Articles: {total_articles}\n")
        count_file.write(f"Total Comments: {total_comments}\n")

def run():
    parser = HTMLParserDoFn()
    with beam.Pipeline() as pipeline:
        (pipeline
         | 'Create file list' >> beam.Create(os.listdir('html_files'))
         | 'Add full path' >> beam.Map(lambda x: os.path.join('html_files', x))
         | 'Parse HTML' >> beam.ParDo(parser)
         | 'Write JSON' >> beam.Map(write_json_file)
        )

    # Write the total counts after the pipeline has finished processing all files
    write_total_counts()

if __name__ == '__main__':
    run()
