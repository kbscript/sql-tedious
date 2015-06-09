var path = require( 'path' );

var ConnManager = require( './ConnManager.js' );
var Request = require( 'tedious' ).Request;
var Params = require( './Params.js' );
var Util = require( 'util' );

var BulkLoad = module.exports = function (conn, tableName, source, callback) {
    var bulkload = this;
    
    if ( typeof callback !== "function" ) { callback = function ( err, result ) { if ( err ) { throw new Error( err ); } } }
    bulkload.callback = callback;
    
    bulkload.tableName = tableName;
    bulkload.source = source;       
    
    bulkload.log = function ( message, type ) { type = type || 'log'; console[type]( message ); };

    bulkload.connmanager = new ConnManager( conn, bulkload.onConnect.bind( bulkload ) );
    bulkload.connmanager.log = bulkload.log;
};

BulkLoad.prototype.onConnect = function ( err, connection ) {
    var bulkload = this, i, column, type, nullable, sqlCreateTable;
    
    if ( err ) { return bulkload.callback( err, bulkload ); }

    bulkload.connection = connection;
    bulkload.bl = connection.newBulkLoad("#" + bulkload.tableName, function ( err, result ) {         
        bulkload.callback( err, bulkload );
    } );
    
    var columns = bulkload.columns = {}
    
    //load source into bl
    for ( i = 0; i < bulkload.source.length; i++ ) {
        //build column definition
        for ( column in bulkload.source[i] ) {
            if ( !bulkload.source[i].hasOwnProperty( column ) ) { continue; }
            
            nullable = typeof bulkload.source[i][column] === "object" && String(bulkload.source[i][column]) === "null"
            type = nullable ? undefined : Params.type( bulkload.source[i][column] );
            length = typeof bulkload.source[i][column] === "string" ? bulkload.source[i][column].length : undefined;
            
            if ( typeof columns[column] === "undefined" ) { 
                columns[column] = { name: column, length: length, nullable: nullable };
            }
            
            //use the widest length of all rows sent
            if ( length && columns[column].length < length ) { columns[column].length = length; }
            //check all rows for a null value - there is one then set column to nullable
            if ( nullable ) { columns[column].nullable = true; }
            //check type - if undefined, then last value was null - wait for a js value
            if ( typeof type !== "undefined" && typeof columns[column].type === "undefined" ) { columns[column].type = type;  }                   
        }        
    } //end loop over source
    
    for ( column in columns ) {
        if ( !columns.hasOwnProperty( column ) ) { continue; }
        
        if ( typeof columns[column].type === "undefined") { columns[column].type = Params.type( "" );  }
        bulkload.bl.addColumn( columns[column].name, columns[column].type, { length: columns[column].length, nullable: columns[column].nullable } );
    } //end loop over columns
    
    for ( i = 0; i < bulkload.source.length; i++ ) { 
        //add row       
        bulkload.bl.addRow( bulkload.source[i] );
    }

    sqlCreateTable = "IF OBJECT_ID('tempdb.dbo.#" + bulkload.tableName + "', 'U') IS NOT NULL DROP TABLE #" + bulkload.tableName + "; "
    sqlCreateTable += bulkload.bl.getTableCreationSql( );
    var request = new Request( sqlCreateTable, function ( err, result ) {
        if ( err ) { return bulkload.callback( "Error creating temp table:  " + err ); } 
        connection.execBulkLoad( bulkload.bl );    
    } );

    connection.execSqlBatch( request );
};



BulkLoad.prototype.close = function () {
    var bulkload = this, connection = bulkload.connection;

    var request = new Request( "Drop TABLE #" + bulkload.tableName, function ( err, count, result ) {
        if ( err ) { console.log( "Error Dropping table." ); console.log( err ); }       

        bulkload.connmanager.pool.release( connection );    
    } );

    connection.execSql( request );
};