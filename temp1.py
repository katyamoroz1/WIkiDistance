from bs4 import BeautifulSoup, SoupStrainer
import requests

from sys import argv
script, string_url, file = argv

url = string_url

page = requests.get(url)
data = page.text
soup = BeautifulSoup(data, features="html.parser")

f = open(file, 'w')
for link in soup.find_all('a'):
    temp = link.get('href')
    if not temp is None and 'http' in temp:
        f.write(temp)
        f.write('\t')
        f.write("\"\"")
        f.write('\n')
f.close()
