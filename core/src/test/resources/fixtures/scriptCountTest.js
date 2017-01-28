var query = db.query('select from OUser');
var count = 0;
query.stream().forEach(function(el){
    count++;
})
count;

