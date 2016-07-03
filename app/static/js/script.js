/*
 * Authors: Julien Ribon [julien@ribon.ch]
 * 
 * Creation: 03.06.2011
 * Modification: 01.04.2016 ... 5 years later :-)
 * Revision: 0.2
 *
 */
 

// select the file input
$('#file').change(function() { 
    // select the form and submit
    $('#upload').submit(); 
});

// DataTable and PivotTable
$(document).ready( function () {
    var derivers = $.pivotUtilities.derivers;
    var renderers = $.extend(
        $.pivotUtilities.renderers, 
        $.pivotUtilities.c3_renderers);
        
    $("#pivot").pivotUI($("#data_table"), {
        renderers: renderers, 
        rendererName: "Table"
    });
    
    //$('#data_table').DataTable();
    //$('#source_table').DataTable();
});


// Filter Modal
// show.bs.modal event fires immediately when the show instance method
// is called, usually caused by a clicked element.
$('[id^=grid].modal').on('show.bs.modal', function (event) {
    var modal = $(this)
    var button = $(event.relatedTarget) // Button that triggered the modal
    var col = button.data('column') // Extract info from data-* attributes
    var op = button.data('operation')
    var title = op.charAt(0).toUpperCase() + op.substr(1).toLowerCase()
    
    // Update the modal's content.
    modal.find('.modal-title').text(title + ': ' + col)
    modal.find('form').attr('action', '/grid/' + col + '/' + op)
    modal.find('button[type=submit]').text(title)
    modal.find('.modal-body input[name=column]').val(col)
});

// Special event for "Split" modal
$('#grid_split_modal').find('select[name=method]').change(function(){
    
    var method_info = $('#grid_split_modal').find('input[name=info]');
    method_info.val('');
    
    if (this.value=='char') {
        method_info.attr('placeholder',',');
        method_info.val(',');
    } else {
        method_info.attr('placeholder',10);
        method_info.val(10);
    }
});
