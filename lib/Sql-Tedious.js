var SqlExecute = require( './SqlExecute.js' );
var Request = require( 'tedious' ).Request;
var BulkLoad = require( './BulkLoad.js' );
var Util = require( 'util' );

var SqlTedious = module.exports = function ( _config ) {
    var sqltedious = this;
    
    //default callback - if none passed to funcitons then we will throw error on done when error is passed.
    sqltedious.done = function ( err ) { if ( err ) { process.nextTick( function () { throw new Error( err ); } ); } };
};

SqlTedious.prototype.query = function ( connection, statement, params, callback ) {
    //pass arguments as statement String, params Object, optional connection Object, optional callback Function 
    var sqltedious = this, useMetaData = false;    
    return new SqlExecute( connection, statement, params, callback );
};

SqlTedious.prototype.insertUpdate = function ( connection, definition, source, params, callback ) {
    var sqltedious = this, i, sqlexecute, sql, mergeSql;
    
    if ( typeof callback !== "function" ) { callback = function ( err ) { if ( err ) { throw new Error( err ); } } }
    if ( typeof definition !== "object" && ( !definition.tableName || !definition.where ) ) { process.nextTick( callback( "A definition parameter with tableName, where and columns is required.", "" ) ); }
    if ( !Util.isArray( source ) || source.length === 0 ) { return process.nextTick( callback.bind( undefined, "A source parameter is required.  ", "" ) ); }   

    //insert source into temp table
    var bulkload = new BulkLoad( connection, definition.tableName, source, function ( err, bulkload ) {
        var mergeSql = buildMergeSql( bulkload, definition );
        var request = new Request( mergeSql, function ( err, count, result ) {
            console.log( result );
            bulkload.close( );
        } );

        bulkload.connection.execSql( request );
    } );
};

SqlTedious.prototype.update = function () { };

SqlTedious.prototype.page = function ( connection, statement, params, callback ) {
    var sqltedious = this, i, sqlexecute, sql, useMetaData = false, sqlString = { select: "", from: "", where: "", orderBy: "" }
    
    if ( typeof callback === "function" ) { sqltedious.done = callback; useMetaData = parseCallbackParams(callback).length > 2 }
    if ( !statement ) { process.nextTick( function () { sqltedious.done( "Invalid arguments.  A sql statement is required.", "" ); } ); }
    
    //set defaults
    if ( !params ) { params = {}; }
    
    if ( params.page ) { params.page = parseInt( params.page, 10 ); }
    if ( params.rows ) { params.rows = parseInt( params.rows, 10 ); }

    if ( isNaN(params.page) || !params.page ) { params.page = 1; }
    if ( isNaN( params.rows ) || !params.rows ) { params.rows = 10; }
    
    sqlString = parseStatement( statement );
    
    if ( sqlString.where ) { sqlString.where = "Where " + sqlString.where; }
    sqlString.select = "Select Row_Number( ) over( order by " + sqlString.orderBy + " ) as rowNum, " + sqlString.select + " "
    
    sql = "Select * From ( \n" + sqlString.select + " \nFrom " + sqlString.from + " " + sqlString.where + " ) as queryTable \n";
    sql += "Where queryTable.rowNum between ((@page-1) * @rows) + 1 And @rows * (@page) \n\n";
    
    sql += "Select count (*) as totalRecords \n From " + sqlString.from + " \n" + sqlString.where;
    
    return sqlexecute = new SqlExecute( sql, params, connection, function ( err, result ) {
            if ( err ) { return callback( err, '' ); }
            
            var total = result[result.length - 1].totalRecords;
            var totalPages = Math.ceil( total / params.rows );
            callback( err, { total: total, pages: totalPages, page: params.page, records: result.splice( 0, result.length - 1 ) } );
    } );    
};

var parseCallbackParams = function ( callback ) {
    var callbackParams = String( callback ).match( /\(.+\)/i ) || [""]
    
    return callbackParams[0].replace( /\(|\)|\s*/g, '' ).split( "," ); 
};

var parseStatement = function ( statement ) {
    var select = statement.match( /(?:\s*select\s)(.+)\s*(?:from)/ig ) || [""];
    var from = statement.match( /(?:\sfrom\s)(.+)\s(?:(where\s|\s$|;$))/ig ) || [""];
    var where = statement.match( /(?:\swhere\s)(.+)\s(?:(order by\s|group by\s|\s$|;$))/ig ) || [""];
    var orderBy = statement.match( /(?:\sorder by\s)(.+)/ig ) || [""];
    
    return {
        select: select[0].replace( /\s*select\s*|\sfrom\s*/ig, "" ),
        from: from[0].replace( /\s*from\s*|\swhere\s*|\s*;\s*$/ig, "" ),
        where: where[0].replace( /\s*where\s*|\sgroup by\s*|\sorder by\s*|\s*;\s*$/ig, "" ),
        orderBy: orderBy[0].replace( /\s*order by\s*|\s*;\s*$/ig, "" )
    }
};

var buildMergeSql = function ( bulkload, definition ) {
    var sql = "", i, column, columns = bulkload.columns;
    
    var sourceColumns = "";
    var targetColumns = "";
    var updateColumns = "";
    var selectColumns = "";
    var oldDataColumns = "";
    var output = "";
    var oldDataSql = ""
    
    for ( column in columns ) {
        if ( selectColumns ) { selectColumns += ", "; }
        selectColumns += column;

        //if definition.columns was passed then we'll filter columns found in source
        if ( Util.isArray( definition.columns ) && definition.columns.length > 0 ) { 
            if ( definition.columns.indexOf( column ) == -1 ) { continue; }
        }
        
        if ( sourceColumns ) { sourceColumns += ", "; }
        sourceColumns += "Source." + column;

        if ( targetColumns ) { targetColumns += ", "; }
        targetColumns += column;

        if ( updateColumns ) { updateColumns += ", "; }
        updateColumns += "Target." + column + " = Source." + column + "\n     ";                

        if ( oldDataColumns ) { oldDataColumns += ", "; }
        oldDataColumns += "Target." + column + " As old_" + column;
    }
    
    sql = "Select Target.* Into #oldData From " + definition.tableName + " As Target Inner Join #" + definition.tableName + " As Source on " + definition.where + " ;\n\n";       
    //build output statement
    output = "Select Source.*, " + oldDataColumns + " \nFrom #" + definition.tableName + " as Source Left outer Join #oldData as Target On " + definition.where + ";\n Drop Table #OldData; ";
     
    sql += "Merge " + definition.tableName + " as Target Using #" + definition.tableName + " As Source \n ";    
    sql += " On " + definition.where + "\n";    
    sql += " When Matched Then Update Set\n     " + updateColumns + "\n";    
    sql += " When Not Matched Then \n";
    sql += "     Insert (" + targetColumns + ") \n";
    sql += "     Values (" + sourceColumns + ");\n";
        
    sql += output;

    return sql;
};