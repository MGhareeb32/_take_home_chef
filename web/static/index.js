function deleteTodo(todoId) {
    fetch('/delete-note', {
        method: 'POST',
        body: JSON.stringify({todoId: todoId})
    }).then((_res) => {
        window.location.href = "/";
    });
}

$(function () {
  $('[data-toggle="tooltip"]').tooltip()
})
