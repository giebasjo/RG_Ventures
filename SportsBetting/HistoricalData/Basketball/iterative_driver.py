from scraping_fn import scrape_nba_stats, nba_stats_info
import numpy as np

# Initialize containers
urls = list(nba_stats_info.keys()); results = list(np.zeros(len(urls))); threads = []

# Test to see if each of these is working -- this should work as of Jan 11, 2019
for idx, url in enumerate( urls[:5] + urls[6:] ):
    scrape_nba_stats( url, results, idx, headless=False )
    print( "\n==== Passed {} ====\n".format(idx+1) )

# Should be head of first dataframe in results list
print( results[0].head() )





