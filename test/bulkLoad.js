var Connection = require( 'tedious' ).Connection;
var Request = require( 'tedious' ).Request;

var config = { server: 'localhost', userName: 'leaGaThAD055', password: 'bb4FFd%#1287DWl$q', options: { database: 'leagueathletics_db', port: 1433, rowCollectionOnRequestCompletion: true } };
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
    
    var bulkLoad = connection.newBulkLoad( '#member_info_temp1', function ( err, result ) {
        if ( err || !result ) { return console.log( err || "Nothing returned." ); }

        var request = new Request( "Select * From #member_info_temp1; Drop Table #member_info_temp1;", function ( err, count, result ) { 
            if ( err || !result ) { return console.log( err || "Nothing returned." ); }
        
        } );

        connection.execSql( request );
    } );
    
    var table = [], i;
    table.push( { id: 1123, first_name: 'Kevin', last_name: 'Barnett' } );
    table.push( { id: 1124, first_name: 'Babs', last_name: 'Barnett' } );
    table.push( { id: 1125, first_name: 'Caleb', last_name: 'Barnett' } );
    table.push( { id: 1126, first_name: 'Jack', last_name: 'Barnett' } );
    
    bulkLoad.addColumn( 'id', TYPES.Int, { nullable: true } );
    bulkLoad.addColumn( 'first_name', TYPES.NVarChar, {length: 30, nullable: true} );
    bulkLoad.addColumn( 'last_name', TYPES.NVarChar, {length: 30, nullable: true} );
    
    for ( i = 0; i < table.length; i++ ) {
        bulkLoad.addRow( table[i] );
    }
    var createTable = bulkLoad.getTableCreationSql( );    
    var request = new Request( createTable, function ( err, count, result ) {
        //if ( err || !result ) { return console.log( err || "Nothing returned." ); }

        connection.execBulkLoad( bulkLoad );
    } );
    
    connection.execSqlBatch( request );
};