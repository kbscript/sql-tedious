var SqlExecute = require( '../lib/SqlExecute.js' );

var settings = require( './settings.json' );
settings.DB1.failover = settings.DB2;
settings.DB2.failover = settings.DB1;

var params = {id: 1234, name: "Kevin Barnett", now: new Date(), AssocID: 10082};
var connection = settings.DB1 || { server: "localhost", database: "dbname", userName: "User", password: "password", options: {} };

var statement = "Select top (1) id From member_info Where Associd = @Associd;"

var counter = 0;

setInterval( function () {     
    var sqlexecute = new SqlExecute( statement, params, connection, function ( err, result ) {
            if ( err ) { return console.log( err ); }
            
            //after 5 successful logins set connection back to 
            console.log( "Success - " + counter );

            counter++;
    } );

}, 20000 );