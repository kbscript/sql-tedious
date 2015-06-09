var Connection = require( 'tedious' ).Connection;
var Request = require( 'tedious' ).Request;

var config = {server: 'localhost', userName: 'leaGaThAD055', password: 'bb4FFd%#1287DWl$q', options: {database: 'leagueathletics_db', port: 1433, rowCollectionOnRequestCompletion: true}};
var connection = new Connection( config );

connection.on( 'connect', function ( err ) {
    if ( typeof run !== "undefined" ) { run( err, connection ); }
    run = undefined;
} );

connection.on( 'error', function ( err ) {
    if ( typeof run !== "undefined" ) { run( err, connection ); }
    run = undefined;
} );

connection.on( 'errorMessage', function ( err ) {
    if ( typeof run !== "undefined" ) { run( err, connection ); }
    run = undefined;
} );

var run = function ( err, connection ) {
    if ( err || !connection ) { return console.log( err || "error connecting" ); }

    var TYPES = require( 'tedious' ).TYPES;        

    var request = new Request( "sp_executesql", function ( err, count, result ) {
        if ( err || !result ) { return console.log( err || "Nothing returned." ); }


    } );
    
    //add params
    var columns = [{ name: 'id', type: TYPES.Int }, { name: 'first_name', type: TYPES.VarChar }, { name: 'last_name', type: TYPES.VarChar }, { name: 'phone', type: TYPES.VarChar }];
    var values = [[1123,'Kevin','Barnett', '5203427729']];
    var table = { columns: columns, rows: values }
    var statement = " Select * From @members; ";
    request.addParameter( 'stmt' , TYPES.NVarChar, statement )
    //request.addParameter( 'params' , TYPES.NVarChar, '@id int' )
    //request.addParameter( 'id' , TYPES.Int, 139506 )
    request.addParameter( 'members' , TYPES.TVP, table )

    connection.callProcedure( request );
};