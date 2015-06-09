var Sql = require( '../lib/Sql-Tedious.js' );
var sql = new Sql( );

var settings = require( './settings.json' );

var params = { id: 1234, name: "Kevin Barnett", now: new Date( ), AssocID: 10082 };
var connection = settings.DB1  // { server: "localhost", database: "dbname", userName: "User", password: "password" };
var statement = "Select top (1) id From member_info Where Associd = @Associd;";

var callback = function ( err, result ) {
    if ( err ) {return console.log( err ); }
    
    console.log( result );
};

//sql.query(connection, statement, params, callback );

//var pageSatement = "Select * From Member_Info Where AssocID = @AssocID Order by First_Name; ";
//sql.page( connection, pageSatement, { id: 2134, AssocID: 10082, page: 1, rows: 10 }, callback );

var where = " Target.id = Source.Id ";
var params = { id: -1 };
var source = [
    { id: 3777528, first_name: "Kevin", last_name: "Barnett", date_birth: null },
    { id: 3777529, first_name: "Babs", last_name: "Barnett", date_birth: null },
    { id: 3777530, first_name: "Caleb", last_name: "Barnett", date_birth: new Date( 2009, 3, 16 ) },
    { id: 3777531, first_name: "Jack", last_name: "Barnett", date_birth: new Date(2011,10,12) }
]

sql.insertUpdate( connection, {tableName:"Member_Info", where: where, columns: ["first_name", "last_name", "date_birth"]}, source, params, callback );