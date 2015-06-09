var path = require( 'path' );

var ConnManager = require( './ConnManager.js' );
var Request = require( 'tedious' ).Request;
var Params = require( './Params.js' );
var Util = require( 'util' );

//global connection list - used to track failover
//properties are made up of server-database-port
var failOverList = {};

var SqlExecute = module.exports = function ( ) {
    var sqlexecute = this
    var args = Array.prototype.slice.call( arguments );
    process.nextTick( sqlexecute.init.bind(sqlexecute, args) );
};

SqlExecute.prototype.init = function ( args ) {
    //pass arguments as statement String, params Object, optional connection Object, optional callback Function
    var sqlexecute = this, i, conn;
        
    //load arguments
    for ( i = 0; i < args.length; i++ ) {
        if ( typeof args[i] === "string" ) { sqlexecute.statement = args[i]; continue; }
        if ( typeof args[i] === "function" ) { sqlexecute.callback = args[i]; continue; }
        if ( typeof args[i] === "object" && args[i].hasOwnProperty( "server" ) && args[i].hasOwnProperty( "userName" ) && args[i].hasOwnProperty( "database" ) && args[i].hasOwnProperty( "password" ) ) {
            conn = args[i];
            continue;
        }
        if ( typeof args[i] === "object" ) { sqlexecute.params = args[i]; }
    } //end args loop
    
    //validate arguments
    var noStatement = "Error: a sql query statement type String is required."
    if ( !sqlexecute.statement ) { sqlexecute.logError( noStatement, "init" ); return sqlexecute.throwError( noStatement ); }
    
    var noConnection = "Error: a connection object is required.  Properties: server: String, user: String, password: String, database: String.  Either setup a default connection when creating a new sql-tedious object or pass a connection object with your query.";
    if ( ( typeof conn !== "object" || !conn.server ) && !sqlexecute.defalutConfig.userName ) { sqlexecute.logError( noConnection, "init" ); return sqlexecute.throwError( noConnection ); }   
    
    if ( typeof sqlexecute.isNull == "undefined" ) { sqlexecute.isNull = null; }
    if ( typeof sqlexecute.metadata === "undefined" ) { sqlexecute.metadata = false; }
    if ( typeof sqlexecute.forceVarchar === "undefined" ) { sqlexecute.forceVarchar = false; }
    sqlexecute.columns = [];
    sqlexecute.typeList = [];
    
    sqlexecute.deadlockCount = 0;
    
    sqlexecute.log = function ( message, type ) { type = type || 'log'; console[type]( message ); };
    
    
    //need to connect here
    sqlexecute.connection = new ConnManager( conn, sqlexecute.exec.bind( sqlexecute ) )
    sqlexecute.connection.log = sqlexecute.log;    
}

SqlExecute.prototype.addParams = function () {
    var sqlexecute = this, params = sqlexecute.params, forceVarchar = sqlexecute.forceVarchar, type;
    if ( typeof sqlexecute.request === "undefined" ) { var err = "Opps, we don't have a valid sql request."; sqlexecute.throwError( err ); return sqlexecute.logError( err, "addParams" ); }
    
    var TYPES = require( 'tedious' ).TYPES
    var param, type;
    for ( param in params ) {
        if ( params.hasOwnProperty( param ) ) {
            forceVarchar = forceVarchar || String( params[param] ) === "null" || typeof params[param] === "undefined";
            type = forceVarchar ? TYPES.VarChar : Params.type( params[param] );          
            sqlexecute.request.addParameter( param, type, params[param] );
        }
    }
};

SqlExecute.prototype.exec = function ( err, connection ) {
    var sqlexecute = this;
    if ( err ) { sqlexecute.logError( err, "exec-done" ); return sqlexecute.done( err, null, "" );}
       
    sqlexecute.prepareFailStatement( );
    
    sqlexecute.request = new Request( sqlexecute.statement, function ( error, count, rows ) { if ( error ) { sqlexecute.logError( error, "exec-new request" ); } sqlexecute.done( error, count, rows, connection ); } );
    if ( sqlexecute.metadata ) sqlexecute.request.on( 'columnMetadata', function ( metadata ) { sqlexecute.buildMetadata( metadata ); } );
    sqlexecute.addParams( );
    
    //then execSql on connetion
    connection.execSql( sqlexecute.request );
    return;                
};

SqlExecute.prototype.prepareFailStatement = function () {
    var sqlexecute = this, prop, fail, name, db, failServer;
    
    for ( prop in failOverList ) {
        if ( !failOverList.hasOwnProperty( prop ) ) { continue; }
        fail = prop.split( "|" );
        name = fail[0];
        db = fail[1];
        failName = failOverList[prop].name;
        
        var serverRegEx = new RegExp( "(\\[|\\s)" + name + "(\\])?\\.(\\[)?" + db + "\\]?" , "gi" );
        
        sqlexecute.statement = sqlexecute.statement.replace( serverRegEx, " [" + failName + "].[" + db + "]" );
    }
      
};

SqlExecute.prototype.buildResult = function ( result ) {
    var sqlexecute = this, i, col, row, returnResult = [], returnRow, regAllowNull = /N$/;
    for ( i = 0; i < result.length; i++ ) {
        row = result[i];
        returnResult[i] = returnRow = {};
        if ( sqlexecute.metadata ) { returnRow._metadata = {}; }
        for ( col in row ) {
            if ( row.hasOwnProperty( col ) ) {
                //check for multiple columns with same name
                if ( Util.isArray( row[col] ) ) {
                    row[col] = row[col][0];
                }
                if ( typeof row[col].metadata === "undefined" ) {
                    continue;
                }
                
                returnRow[row[col].metadata.colName] = row[col].value;
                if ( row[col].value === null ) { row[col].value = sqlexecute.isNull; }
                
                if ( !sqlexecute.metadata ) { continue; }
                
                returnRow._metadata[row[col].metadata.colName] = {};
                
                returnRow._metadata[row[col].metadata.colName].name = row[col].metadata.colName;
                returnRow._metadata[row[col].metadata.colName].type = row[col].metadata.type.name.replace( regAllowNull, "" );
                returnRow._metadata[row[col].metadata.colName].length = row[col].metadata.dataLength;
                returnRow._metadata[row[col].metadata.colName].allowNull = regAllowNull.test( row[col].metadata.type.name );
            }
        }//end col loop in row
    }//end loop on result         
    
    return returnResult;
};

SqlExecute.prototype.buildMetadata = function ( metadata ) {
    var sqlexecute = this, i;
    for ( i = 0; i < metadata.length; i++ ) {
        //load column names into columns array, returned when statement complete even if no rows
        sqlexecute.columns.push( metadata[i].colName );
        // load column type into typeList array, returned when statement complete even if no rows
        var typeObj = {};
        typeObj[metadata[i].colName] = metadata[i].type.name;
        sqlexecute.typeList.push( typeObj );
    }
}

SqlExecute.prototype.done = function ( error, count, rows, connection ) {
    var sqlexecute = this;
    
    if ( !error ) {
        sqlexecute.connection.pool.release( connection );
        if ( typeof sqlexecute.callback !== 'function' ) { return }
        return sqlexecute.callback( "", sqlexecute.buildResult( rows ), sqlexecute.typeList, sqlexecute.columns );
    }
    
    //do retries we got deadlock error
    if ( String( error ).match( /deadlock/gi ) && connection && sqlexecute.deadlockCount < 5 ) {
        //then execSql on connetion
        sqlexecute.logError( "Deadlock, retry: " + sqlexecute.deadlockCount + ", statement: " + sqlexecute.statement, "Deadlock" );
        sqlexecute.deadlockCount += 1
        return sqlexecute.exec( "", connection );
    }
    
    //then kill
    sqlexecute.connection.pool.release( connection );
    sqlexecute.throwError( error );
};

SqlExecute.prototype.throwError = function ( error ) {
    var sqlexecute = this;
    
    if ( typeof sqlexecute.callback === "function" ) { return sqlexecute.callback( error, "" ); }
    
    //else if we got here then no callback is provided and we default to an unhandled error
    throw new Error( error )
};

SqlExecute.prototype.logError = function ( error, location ) {
    sqlexecute = this;
    
    sqlexecute.log( location, "error" );
    if ( sqlexecute._connection ) { sqlexecute.log( "Connection: Server-" + sqlexecute._connection.server + " Database-" + sqlexecute._connection.database + " Name-" + sqlexecute._connection.name, "error" ); }
    if ( sqlexecute._connection && sqlexecute._connection.failover ) { sqlexecute.log( "Connection: Server-" + sqlexecute._connection.failover.server + " Database-" + sqlexecute._connection.failover.database + " Name-" + sqlexecute._connection.failover.name, "error" ); }
    sqlexecute.log( "Statement: " + sqlexecute.statement, "error" );
    sqlexecute.log( error, "error" );
};

var deepExtend = function ( child, parent ) {
    var key, __hasProp = Object.hasOwnProperty;
    for ( key in parent ) {
        if ( __hasProp.call( parent, key ) ) {
            if ( typeof parent[key] !== "object" ) { child[key] = parent[key]; continue; }
            
            if ( Util.isArray( parent[key] ) ) { child[key] = []; deepExtend( child[key], parent[key] ); continue; }
            if ( Util.isDate( parent[key] ) ) { child[key] = new Date( parent[key].getTime( ) ); continue; }
            if ( parent[key] === null ) { child[key] = null; continue; }
            if ( parent[key] instanceof RegExp ) { child[key] = parent[key]; continue; }
            
            child[key] = {};
            deepExtend( child[key], parent[key] );
        }
    }
}