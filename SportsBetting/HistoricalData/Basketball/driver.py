from scraping_fn import scrape_nba_stats, nba_stats_info
import numpy as np
from threading import Thread

urls = list(nba_stats_info.keys()); results = list(np.zeros(len(urls))); threads = []

for idx, url in enumerate( urls[:5] + urls[6:] ):
    scrape_nba_stats( url, results, idx, headless=False )
    print( "\n==== Passed {} ====\n".format(idx+1) )


# for idx, url in enumerate( urls[:5] + urls[6:] ):
#     # We start one thread per url present.
#     process = Thread( target=scrape_nba_stats, args=[url, results, idx, True], name="t{}".format(idx+1) )
#     process.start()
#     threads.append(process)
#
# for process in threads:
#     process.join()

# Should be head of first dataframe in results list
print( results[0].head() )





