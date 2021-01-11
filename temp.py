from bs4 import BeautifulSoup, SoupStrainer
import requests

from sys import argv
script, string_url, file = argv

url = string_url

page = requests.get(url)
data = page.text
soup = BeautifulSoup(data, features="html.parser")

f = open(file, 'a')
for link in soup.find_all('a'):
    temp = ''.join(str(link.get('href')).split())

    if not temp is None and 'http' in temp:
        f.write(temp)
        f.write('\t')
        f.write("0")
        f.write('\n')
f.close()
