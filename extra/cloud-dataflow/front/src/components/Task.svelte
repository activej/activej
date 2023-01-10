<script>
    import {onDestroy, onMount} from 'svelte';
    import Graph from './Graph.svelte';
    import {updating} from '../App.svelte'

    export let partitions;
    export let task;

    let selectedPartition = -1;

    let taskData, overviewData;

    function formatDuration(millis) {
        if (millis < 1000) {
            return '00:00';
        }
        const secs = millis / 1000;
        const days = Math.floor(secs / 86400);
        const hours = Math.floor((secs - days * 86400) / 3600);
        const minutes = Math.floor((secs - days * 86400 - hours * 3600) / 60);
        const seconds = Math.floor(secs - days * 86400 - hours * 3600 - minutes * 60);
        const pad = n => n.toString().padStart(2, '0');
        return (days !== 0 ? `${pad(days)}:${pad(hours)}:` : hours !== 0 ? `${pad(hours)}:` : '') +
                `${pad(minutes)}:${pad(seconds)}`;
    }

    function updateTaskData() {
        if (selectedPartition === -1) {
            return fetch(`/api/tasks/${task}`)
                    .then(r => r.json())
                    .then(r => overviewData = r);
        } else {
            return fetch(`/api/tasks/${task}/${selectedPartition}`)
                    .then(r => r.json())
                    .then(r => taskData = r);
        }
    }
    $: updateTaskData(selectedPartition);

    let timerId = -1;
    let timeDisplayTimerId;

    async function startUpdating() {
        await updateTaskData();
        if (timerId) {
            timerId = setTimeout(startUpdating, 1000);
        }
    }

    function stopUpdating() {
        clearInterval(timerId);
        timerId = null;
    }

    $: ($updating ? startUpdating : stopUpdating)();

    const timeListeners = [];
    const timeSource = {
        subscribe: cb => {
            timeListeners.push(cb);
            return () => {
                const index = timeListeners.indexOf(5);
                if (index !== -1) {
                    timeListeners.splice(index, 1);
                }
            }
        }
    }

    onMount(() => {
        let start = Date.now();
        timeDisplayTimerId = setInterval(() => {
            start += 1000;
            timeListeners.forEach(listener => listener(start));
        }, 1000);
    });
    onDestroy(() => {
        stopUpdating();
        clearInterval(timeDisplayTimerId);
    });
</script>

<div class="row no-gutters">
    <div class="col-auto d-flex">
        <div class="nav nav-tabs">
            <div class="nav-item">
                <div class="nav-link h-100 pointer"
                     class:active={selectedPartition === -1}
                     on:click={() => selectedPartition = -1}>
                    Overview
                </div>
            </div>
        </div>
    </div>
    <div class="col overflow-auto">
        <ul class="nav nav-tabs flex-nowrap">
            {#each partitions as partition, i}
                <li class="nav-item">
                    <div class="nav-link pointer"
                         class:active={selectedPartition === i}
                         on:click={() => selectedPartition = i}>
                        {partition}
                    </div>
                </li>
            {/each}
        </ul>
    </div>
</div>

<div class="row">
    {#if selectedPartition === -1}
        <div class="col">
            {#if overviewData}
                <div>Statuses: {overviewData.statuses}</div>
                <Graph graphviz={overviewData.graph}
                       index={selectedPartition}
                       nodeStats="{overviewData.reducedNodeStats}"/>
            {:else}
                <div>waiting for task data...</div>
            {/if}
        </div>
    {:else}
        <div class="col">
            {#if taskData}
                <div>Status: {taskData.status}</div>
                {#if taskData.started}
                    <div>Started: {new Date(taskData.started).toLocaleString('en-GB')}</div>
                    {#if taskData.finished}
                        <div>Finished: {new Date(taskData.finished).toLocaleString('en-GB')}</div>
                        <div>Task took {formatDuration(taskData.finished - taskData.started)} to execute</div>
                    {:else}
                        <div>Task is running for {formatDuration($timeSource - taskData.started)}</div>
                    {/if}
                {/if}
                <Graph graphviz={taskData.graph}
                       index={selectedPartition}
                       nodeStats="{taskData.nodeStats}"/>
            {:else}
                <div>waiting for task data...</div>
            {/if}
        </div>
    {/if}
</div>
