from scraping_fn import scrape_nba_stats, nba_stats_info
import numpy as np
from threading import Thread

# Initialize containers
urls = list(nba_stats_info.keys()); results = list(np.zeros(len(urls))); threads = []

# Initialize threads and assign proper arguments
for idx, url in enumerate( urls[:5] + urls[6:] ):
    # We start one thread per url present.
    process = Thread( target=scrape_nba_stats, args=[url, results, idx, True], name="t{}".format(idx+1) )
    process.start()
    threads.append(process)

# Merge threads
for process in threads:
    process.join()

# Should be head of first dataframe in results list
print( results[0].head() )





