var TYPES = require( 'tedious' ).TYPES;
var Util = require( 'util' );
var convert = exports.type = function ( param ) {    
    if ( String( param ) === "null" || typeof param === "undefined" || typeof param === "string" ) { return TYPES.VarChar; }
    
    if ( Util.isDate( param ) ) { return TYPES.DateTime; }
    
    if ( typeof param === "boolean" ) { return TYPES.Bit; }
    if ( typeof param === "number" && String( param ).indexOf( "." ) !== -1 && !isNaN( parseFloat( param ) ) ) { return TYPES.Float; }
    if ( typeof param === "number" && !isNaN( parseInt( param, 10 ) ) ) { return TYPES.Int; }
    
    return TYPES.VarChar;       
};