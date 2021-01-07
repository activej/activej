<script>
    export let bytes;

    function formatBytes(bytes) {
        if (bytes === 0) return '0 bytes';
        if (bytes === 1) return '1 byte';
        const power = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, power)).toFixed(2) + ' ' + ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'][power];
    }

    function addSpaces(x) {
        return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
    }
</script>

<style>
    span {
        cursor: default;
    }
</style>

<div>transfered <span class="text-muted pointer" title="{addSpaces(bytes)} bytes">{formatBytes(bytes)}</span> of data</div>
