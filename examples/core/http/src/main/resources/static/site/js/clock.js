function updateClock() {
    var now = new Date();
    var time = now.getHours() + ':' + now.getMinutes() + ':' + now.getSeconds();
    console.log(time);
    document.getElementById('time').innerHTML = time;

    setTimeout(updateClock, 1000);
}
