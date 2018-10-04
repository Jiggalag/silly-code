from bs4 import BeautifulSoup

# url = 'http://40.127.165.240/#/events/filter/limit=500'
# filepath = '/home/jiggalag/Desktop/events.html'
filepath = '/home/jiggalag/Desktop/events3000.html'

with open(filepath, 'r') as f:
    html = f.read()

parsed_html = BeautifulSoup(html, 'lxml')
text = parsed_html.contents[1].text
mismatch = text.count('Нет совпадений')
match = text.count('%)')
print('ok')
