# DevHubRecs
 Recommendation engine for DevHub articles

Pre-work: save a TSV file downloaded from GA query explorer demo/tools
             https://ga-dev-tools.appspot.com/query-explorer/


parseTsvToCsv.py - step 1, cleans/prepares data. input:tsv file, output: csv file with the same name
writeDataToMongo.py - step 2, takes cleaned up csv file and imports it into the pageviews collection.
                      This collection has a record for each user with an array of pages they visited.
                      New data is merged with existing data and de-duped
associate.py - step 3, does the associative work - users that went to X page also went to Y page. 
               Stores data in the associations collection. Calculated new each time, drops old data.
getAssociations.py - Using information from the associations collection, make an array for each URL that has associations


compareAssociations.py - Compare the associations collection with the related collection, display differences.
                         Data is not stored anywhere or changed; this allows us to easily see the added value
                         of this process.
