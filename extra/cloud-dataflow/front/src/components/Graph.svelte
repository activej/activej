<script context="module">
    import Viz from 'viz.js';
    import renderer from 'viz.js/lite.render.js';

    export const graphKey = {};

    const svgCache = new Map();

    let viz = new Viz(renderer);

    function render(gv) {
        return viz.renderSVGElement(gv)
                .then(svg => {
                    svgCache.set(gv, svg);
                    return svg;
                }, e => {
                    viz = new Viz(renderer); // this is needed according to viz.js docs
                    console.log(e);
                });
    }
</script>

<script>
    import {onMount} from 'svelte';
    import {setContext} from 'svelte';
    import {get} from 'svelte/store';
    import {delay, persistent} from '../util';

    import Modal from './Modal.svelte';
    import FallbackStat from './stats/FallbackStat.svelte';

    export let graphviz;
    export let index;
    export let nodeStats;

    let modals = [];
    let modalMap = new Map();

    let container;

    let svg;
    let baseX, baseY;
    let x, y, scale;
    let mOffX = 0, mOffY = 0;

    let coords;

    $: updateGraphCoords(index); // propely update coords and their storage when svelte reuses this component to show a different graph
    $: updateModalArgs(nodeStats); // update modal args when nodeStats gets updated
    $: updateTransform(svg, baseX, baseY, x, y, scale, dragging); // call updateTransform whenever listed variables change

    let _prevIndex = null;

    function updateGraphCoords() {
        coords = persistent(localStorage, `coords_${index}`, {x: 0, y: 0, scale: 1, dragging: false});
        const value = $coords;
        x = value.x;
        y = value.y;
        scale = value.scale;
        dragging = value.dragging;

        // this runs only when the index is triggered, so we also reset the modals here too when it actually changes
        if (index !== _prevIndex) {
            if (_prevIndex !== null) {
                modals = [];
                modalMap.clear();
                console.log("cleared modals!");
            }
            _prevIndex = index;
        }
    }

    function updateModalArgs() {
        for (const modal of modals) {
            const stats = nodeStats[modal.nodeIndex];
            stats.$type = stats.$type.startsWith(".") ? stats.$type.substring(1) : stats.$type;
            modal.args = stats;
        }
        modals = modals;
    }

    async function getBBox(svg) {
        let bbox;
        do { // waiting for browser to calculate that bbox for us, ugh
            bbox = svg.getBBox();
            await delay(10);
        } while (bbox.x === 0 && bbox.y === 0 && bbox.width === 0 && bbox.height === 0);
        return bbox;
    }

    function recalculateBasis(svg, container, bbox) {
        const containerBBox = container.getBoundingClientRect();

        if (bbox.width < containerBBox.width && bbox.height < containerBBox.height) {
            baseX = containerBBox.width / 2 - bbox.width / 2;
            baseY = containerBBox.height / 2 - bbox.height / 2;
        } else {
            let highestBbox = null;
            svg.querySelectorAll('g.node').forEach(n => {
                const bbox = n.getBBox();
                if (!highestBbox || bbox.y < highestBbox.y) {
                    highestBbox = bbox;
                }
            });
            highestBbox = highestBbox || bbox;
            baseX = -highestBbox.x - highestBbox.width / 2 + containerBBox.width / 2;
            baseY = -highestBbox.y - bbox.height - highestBbox.height / 2 + containerBBox.height / 6;
        }
    }

    async function onNodeClick(nodeName, nodeIndex, rx, ry) {
        const existing = modalMap.get(nodeIndex);
        if (existing) {
            removeModal(existing);
            return;
        }
        const stats = nodeStats[nodeIndex];
        if (!stats) {
            return; // no stats for this node
        }
        stats.$type = stats.$type.startsWith(".") ? stats.$type.substring(1) : stats.$type;
        // there needs to be a component stats/statType.svelte, or it will just show args (and type) as json
        const component = await import(`./stats/${stats.$type}.svelte`).then(it => it.default, () => FallbackStat);

        const modal = {x: rx - x - mOffX, y: ry - y - mOffY, args: stats, component, nodeName, nodeIndex};
        modalMap.set(nodeIndex, modal);
        modals = [...modals, modal];
    }

    async function update(gv) {
        if (!container) { // update is called before onMount once, when the container is not yet populated
            return; // same with gv
        }

        // if it was already set up then just skip setting it up again
        const existing = svgCache.get(gv);
        if (existing) {
            existing.style.visibility = 'hidden';
            container.appendChild(existing);
            recalculateBasis(existing, container, await getBBox(existing));
            if (svg) {
                svg.parentElement.removeChild(svg);
            }
            existing.style.visibility = null;
            svg = existing;
            return;
        }

        // else render it and set it up
        const newSvg = await render(gv);
        svgCache.set(gv, newSvg);

        newSvg.classList.add('graph-svg');

        newSvg.querySelectorAll('title').forEach(it => it.parentElement.removeChild(it)); // remove all the titles
        newSvg.querySelectorAll('g.graph > polygon').forEach(it => it.parentElement.removeChild(it)); // remove white background

        if (!container) { // actually idk how it might be null here, async stuff?
            return;
        }

        const containerRect = container.getBoundingClientRect();
        newSvg.addEventListener('click', async e => {
            const target = e.target;
            if (target.tagName.toLowerCase() !== 'text') { // well, SVG tags are lowercased it looks like, at least in Firefox
                return;
            }
            try {
                const nodeIndex = parseInt(target.parentElement.id.substring(1));
                await onNodeClick(target.innerHTML, nodeIndex, e.pageX - containerRect.x, e.pageY - containerRect.y);
            } catch (e) {
                console.log(e);
            }
        });

        newSvg.style.visibility = 'hidden';
        container.appendChild(newSvg);

        const bbox = await getBBox(newSvg);

        // set the size of svg to match its fixed contents (actual nodeStats and stuff)
        newSvg.setAttribute('width', (bbox.width + 2).toString());
        newSvg.setAttribute('height', (bbox.height + 2).toString());
        newSvg.setAttribute('viewBox', [bbox.x - 2, bbox.y - 2, bbox.width + 4, bbox.height + 4].join(' '));

        recalculateBasis(newSvg, container, bbox);
        if (svg) {
            svg.parentElement.removeChild(svg);
        }
        newSvg.style.visibility = null;
        svg = newSvg;
    }

    $: update(graphviz);
    // ^ first update won't do anything as the container is still undefined and there is a check for that
    // onMount will be called when container is bound, so we also call update here
    onMount(() => update(graphviz));

    function resetTransform() {
        x = 0;
        y = 0;
        scale = 1;
    }

    function updateTransform() {
        if ([svg, baseX, baseY, x, y, scale].filter(it => it === undefined).length !== 0) {
            return;
        }
        svg.style.transform = 'translate(' + (baseX + x) + 'px, ' + (baseY + y) + 'px) scale(' + scale + ')';
        coords.set({x, y, scale, dragging});
    }

    let prevX, prevY, dragging = false;

    function mousedown(e) {
        prevX = e.pageX;
        prevY = e.pageY;
        dragging = true;
    }

    function mousemove(e) {
        if (!dragging) {
            return;
        }
        if (e.buttons === 0) {
            mouseup();
            return
        }
        const dx = e.pageX - prevX;
        const dy = e.pageY - prevY;
        if (dragging.modal) {
            dragging.updatePos(x + mOffX + (dragging.modal.x += dx), y + mOffY + (dragging.modal.y += dy));
        } else {
            x += dx;
            y += dy;
        }
        prevX = e.pageX;
        prevY = e.pageY;
    }

    function mouseup() {
        dragging = false;
    }

    function mousewheel(e) {
        let nextScale = scale;
        if ((e.detail !== 0 ? e.detail : e.deltaY) > 0) {
            if (nextScale > 0.1) {
                nextScale /= 1.1;
            }
        } else if (nextScale < 10) {
            nextScale *= 1.1;
        }
        const box = svg.getBoundingClientRect();
        const scaleRatio = nextScale / scale - 1;
        const scaleOffX = (box.x + box.width / 2 - e.pageX) * scaleRatio;
        const scaleOffY = (box.y + box.height / 2 - e.pageY) * scaleRatio;
        x += scaleOffX;
        y += scaleOffY;
        mOffX -= scaleOffX;
        mOffY -= scaleOffY;

        for (const modal of modals) {
            if (!modal.container) {
                continue;
            }
            const box = modal.container.getBoundingClientRect();
            modal.x += (box.x + box.width / 2 - e.pageX) * scaleRatio;
            modal.y += (box.y + box.height / 2 - e.pageY) * scaleRatio;
        }
        modals = modals;
        scale = nextScale;
        return false;
    }

    function dragModal(e, modal, updatePos) {
        e.stopPropagation();
        mousedown(e);
        dragging = {modal, updatePos};
        modals = [...modals.filter(m => m !== modal), modal];
    }

    function removeModal(modal) {
        modals = modals.filter(m => m !== modal);
        modalMap.delete(modal.nodeIndex);
    }

    setContext(graphKey, {dragModal, removeModal});
</script>

<div class="graph-container"
     bind:this={container}
     on:mousedown={mousedown}
     on:mousemove={mousemove}
     on:mouseup={mouseup}
     on:mousewheel|preventDefault={mousewheel}
     on:DOMMouseScroll|preventDefault={mousewheel}> <!-- because Firefox is weird -->
    <div class="graph-info m-1">
        <svg class="reset" width="16" height="16" on:click={resetTransform}>
            <rect x="10" y="6.5" width="5" height="2" fill="#99f"/>
            <rect x="0" y="6.5" width="5" height="2" fill="#99f"/>
            <rect x="6.5" y="0" width="2" height="5" fill="#99f"/>
            <rect x="6.5" y="10.5" width="2" height="5" fill="#99f"/>
            <rect x="6.5" y="6.5" width="2" height="2" fill="#99f"/>
        </svg>
        <div>pos: {x.toFixed()}:{y.toFixed()}</div>
        <div>scale: {scale.toFixed(2)}</div>
    </div>
    {#each modals as modal (modal.nodeIndex)}
        <Modal x="{x + modal.x + mOffX}" y="{y + modal.y + mOffY}" it="{modal}"/>
    {/each}
</div>

<style>
    :global(.graph-svg) { /* global so it won't get mangled */
        user-select: none;
        /*pointer-events: none;*/
        position: absolute;
    }

    .reset {
        position: relative;
        z-index: 1;
    }

    /* trick to make it square with a side of 'width: 100%' */
    .graph-container:before {
        content: "";
        float: left;
        padding-top: 100%;
    }

    .graph-container {
        position: absolute;
        border: inset 2px lightgray;
        background: #fafafa;
        overflow: hidden;
        width: 100%;
    }

    .graph-info {
        position: absolute;
        user-select: none;
        font-size: 0.75em;
    }
</style>

