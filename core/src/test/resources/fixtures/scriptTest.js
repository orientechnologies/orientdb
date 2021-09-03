var query = db.query('select from OUser');
var res = [];
query.stream().forEach(function(el){
    res.push(el.getElement().get());
});
res;
