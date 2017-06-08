SELECT FROM City WHERE name LUCENE 'rome'



SELECT FROM City WHERE SEARCH_CLASS('rome') = true
SELECT FROM City WHERE SEARCH_FIELDS(['name'], 'rome') = true
SELECT FROM City WHERE SEARCH_INDEX('City.name', 'rome') = true


SELECT FROM City WHERE SEARCH_MORE([#25:2, #25:3],{'minTermFreq':1, 'minDocFreq':1} ) = true


SELECT EXPAND(SEARCH('City.name:rome Person.name:John') )