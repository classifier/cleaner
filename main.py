from textblob import TextBlob
import json
from joblib import Parallel, delayed
import multiprocessing

f = open('yelp_academic_dataset_review.json')

fr = open('yelp_result.json', 'w')


def extract_words(line):
    review = json.loads(line)
    b = TextBlob(review['text'])
    d = {'stars': review['stars'], 'words': b.words.lower()}
    return json.dumps(d)


def job(line):
    fr.write(extract_words(line) + '\n')


def main():
    num_cores = multiprocessing.cpu_count()
    Parallel(n_jobs=num_cores)(delayed(job)(line) for line in f)

if __name__ == '__main__':
    main()
