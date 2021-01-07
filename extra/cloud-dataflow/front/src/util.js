import {writable} from 'svelte/store';

export function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function persistent(storage, name, defaultValue) {
    const store = writable(JSON.parse(storage.getItem(name)) || defaultValue);
    store.subscribe(value => storage.setItem(name, JSON.stringify(value)));
    return store;
}
