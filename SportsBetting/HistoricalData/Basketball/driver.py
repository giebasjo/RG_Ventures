from scraping_fn import scrape_nba_stats, nba_stats_info
import numpy as np
from threading import Thread

# for idx, url in enumerate( (nba_stats_info.keys()) ):
#     scrape_nba_stats( url )
#     print( "\n==== Passed {} ====\n".format(idx+1) )
#

urls = list(nba_stats_info.keys()); results = list(np.zeros(len(urls))); threads = []
for idx, url in enumerate( urls ):
    # We start one thread per url present.
    process = Thread( target=scrape_nba_stats, args=[url, results, idx], name="t{}".format(idx+1) )
    process.start()
    threads.append(process)

for process in threads:
    process.join()

# Should be head of first dataframe in results list
print( results[0].head() )
