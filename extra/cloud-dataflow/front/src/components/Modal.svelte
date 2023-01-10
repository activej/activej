<script>
    import {getContext, onMount} from 'svelte';
    import {graphKey} from './Graph.svelte';

    export let x, y;
    export let it;
    let container;

    $: it.container = container;

    function updatePos(x, y) {
        if (!container) {
            return;
        }
        container.style.left = x + 'px';
        container.style.top = y + 'px';
    }

    const {dragModal, removeModal} = getContext(graphKey);

    $: updatePos(x, y);
    onMount(() => updatePos(x, y));
</script>

<div class="position-absolute text-nowrap"
     bind:this="{container}">
    <div class="header d-flex justify-content-between unselectable px-1" on:mousedown={e => dragModal(e, it, updatePos)}>
        <span class="w-100 text-center">{it.nodeName} ({it.nodeIndex})</span>
        <span class="pointer" on:click={() => removeModal(it)}>
            <svg width="18" height="18" viewBox="0 0 18 18">
                <path d="M14.53 4.53l-1.06-1.06L9 7.94 4.53 3.47 3.47 4.53 7.94 9l-4.47 4.47 1.06 1.06L9 10.06l4.47 4.47 1.06-1.06L10.06 9z"/>
            </svg>
        </span>
    </div>
    <div class="body px-1" on:mousedown={e => e.stopPropagation()}>
        <svelte:component this="{it.component}" {...it.args}/>
    </div>
</div>

<style>
    div.header {
        background: #e6e6e6;
        border-radius: 5px 5px 0 0;
        border: 1px solid gray;
        border-bottom: none;
    }

    div.body {
        background: white; /* so that it is not transparent */
        border-radius: 0 0 5px 5px;
        border: 1px solid gray;
    }
</style>
