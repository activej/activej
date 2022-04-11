<script context="module">
    import {persistent} from './util';

    export const updating = persistent(localStorage, 'updating', true);
</script>

<script>
    import Task from './components/Task.svelte';

    let tasks = [];
    let partitions = [];

    let currentTask;
    let taskComponent;

    let container;
    let loading = true;

    async function onload() {
        try {
            for (const partition of await fetch('/api/partitions').then(r => r.json())) {
                partitions = [...partitions, partition];
            }
            for (const id of Object.keys(await fetch('/api/tasks').then(r => r.json()))) {
                tasks = [...tasks, {id}];
            }
        } finally {
            // and show the container when everything is done
            loading = false;
        }
    }
</script>

<style>
    .loading {
        display: none !important; /* !important because of d-flex */
    }
</style>

<svelte:window on:load={onload}/>

<div bind:this={container} class="container h-100 d-flex flex-column" class:loading>
    <div class="row">
        <div class="col pt-1">
            <h3>Dataflow debug console</h3>
        </div>
    </div>
    <div class="row flex-grow-1">
        <div class="col-2">
            {#if taskComponent}
                <div class="card mb-3">
                    <div class="card-header py-1 text-center">Controls</div>
                    <div class="card-body p-0 d-flex justify-content-center">
                        <button class="btn btn-outline-secondary m-2" class:active={$updating}
                                on:click={() => updating.set(!$updating)}>
                            auto-update
                        </button>
                    </div>
                </div>
            {/if}
            <div class="card mb-3">
                <div class="card-header py-1 text-center">Tasks</div>
                <div class="card-body p-0">
                    <div class="list-group list-group-flush">
                        {#each tasks as {id}}
                            <div on:click="{() => currentTask = id}"
                                 class:active="{currentTask === id}"
                                 class="list-group-item py-1 list-group-item-action pointer text-center">{id}</div>
                        {/each}
                    </div>
                </div>
            </div>
            <div class="card">
                <div class="card-header py-1 text-center">Partitions</div>
                <div class="card-body p-0">
                    <div class="list-group list-group-flush">
                        {#each partitions as partition}
                            <div class="list-group-item py-1 text-center">{partition}</div>
                        {/each}
                    </div>
                </div>
            </div>
        </div>
        <div class="col">
            {#if currentTask}
                <Task partitions={partitions} task={currentTask} bind:this={taskComponent}/>
            {/if}
        </div>
    </div>
</div>
