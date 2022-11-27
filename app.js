const express = require('express');
const app = express();
const router = express.Router();
const fs = require("fs");
const path = __dirname + '/views/';
const pathArbitrajes = __dirname + '/ArbitrajeMultiple/';
const port = 8080;

router.use(function (req,res,next) {
  console.log('/' + req.method);
  next();
});

router.get('/', function(req,res){
  res.sendFile(path + 'index.html');
});

router.get('/sharks', function(req,res){
  res.send("hola")
  //res.sendFile(path + 'sharks.html');
});

router.get('/arbitrajes_html', function(req,res){
  //res.send("hola")
  res.sendFile(pathArbitrajes + 'output.html');
});
router.get('/arbitrajes', function(req,res){
  let data = fs.readFileSync(pathArbitrajes +'output.csv', "utf8")
  data = data.split("\n")
  data=data.map((tr)=> '<tr>'+ tr.split(',').map((th) => '<th>'+th+'</th>').join('') +'</tr>')
  data='<table>'+data.join('')+'</table>'
  data='<div>'+data+'</div>'
  data='<body>'+data+'</body>'
  
  res.send(data);
});


app.use(express.static(path));
app.use('/', router);

app.listen(port, function () {
  console.log('Example app listening on port '+port)
})