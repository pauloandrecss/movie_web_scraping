import requests
import time
import csv
import random
import concurrent.futures

from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 '
                  'Safari/537.36'}

MAX_THREADS = 10


# Função que extrai os dados dos filmes utilizando o metodo find para achar os dados pela demarcação HTML e os coloca
# em um arquivo .csv
def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = BeautifulSoup(requests.get(movie_link, headers=headers).content, 'html.parser')
    movie_soup = response

    if movie_soup is not None:
        title = None
        date = None

        movie_data = movie_soup.find('div', attrs={'class': 'sc-52d569c6-0'})
        if movie_data is not None:
            title = movie_data.find('span', attrs={'class': 'sc-afe43def-1'}).get_text()
            date = movie_data.find('a', attrs={'class': 'ipc-link'}).get_text().strip()

        rating = movie_soup.find('span', attrs={'class': 'sc-bde20123-1'}).get_text() if movie_soup.find(
            'span', attrs={'class': 'sc-bde20123-1'}) else None

        plot_text = movie_soup.find('span', attrs={'class': 'sc-cd57914c-0'}).get_text().strip() if movie_soup.find(
            'span', attrs={'class': 'sc-cd57914c-0'}) else None

        with open('movies.csv', mode='a') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if all([title, date, rating, plot_text]):
                print(title, date, rating, plot_text)
                movie_writer.writerow([title, date, rating, plot_text])


# Função que extrai um filme individualmente e chama a função anterior para extrair os detalhes de cada filme
def extract_movies(soup):
    movies_table = soup.find('table', attrs={'data-caller-name': 'chart-moviemeter'}).find('tbody')
    movies_table_rows = movies_table.find_all('tr')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)


# Função main que faz o start de tudo para começar as nossas funções fornecendo os dados necessarios
def main():
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()
