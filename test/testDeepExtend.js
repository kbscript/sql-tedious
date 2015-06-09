var Util = require( 'util' );

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

var test1 = {
    prop1: { name: 'Kevin', email: 'test@test.com', created: new Date( ) },
    prop2: "test",
    prop3: new Date( ),
    prop4: /^test/gi,
    prop5: [{g:1,t:2}, {g:4, t: 5}]
}

var test2 = {};

deepExtend( test2, test1 );

test2.prop5[0].t = 100;
console.log( test1.prop5[0].t );
console.log( test2.prop5[0].t );

test2.prop3 = new Date( '2015-01-01' );
console.log( test2.prop3 );
console.log( test1.prop3 );

console.log( test2 );