class TodoService {
    static loadList() {
        return fetch('http://localhost:8080/get/all')
            .then(response => response.json());
    }

    static sendNewRecord(item) {
        console.log(item);
        return fetch('http://localhost:8080/add', {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                title: item.title,
                plans: [{
                    text : item.plans[0],
                    isComplete : false
                }, {
                    text : item.plans[1],
                    isComplete : false
                }]
            })
        })
    }

    static toggle(id, planId) {
       return fetch('http://localhost:8080/toggle/' + id + "/" + planId);
    }

    static delete(id) {
        return fetch('http://localhost:8080/delete/' + id);
    }
}

export default TodoService;
