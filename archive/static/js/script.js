/*
 * Authors: Julien Ribon [julien@ribon.ch]
 * 
 * Creation: 03.06.2011
 * Modification: 01.04.2016 ... 5 years later :-)
 * Revision: 0.2
 *
 */
 
/*
$('#refresh').click( function() { 
    $("#table").load("/indexing");
});

$('#upload').click( function() { 
    $("#list").load("/upload");
});
 
$(document).ready(function(){

});
*/

/*
$('#list a').click(function(){
    $('#list a').css('font-weight', 'normal');
    $('#list a').css('font-style', 'normal');
    $(this).css('font-weight', 'bold');
    $(this).css('font-style', 'italic');
    
    $('#doc').text($(this).find('span:first').text());
});
*/

/*
// select the file input (using a id would be faster)
$('input[type=file]').change(function() { 
    // select the form and submit
    $('form').submit(); 
});
*/

// select the file input
$('#file').change(function() { 
    // select the form and submit
    $('#upload').submit(); 
});
