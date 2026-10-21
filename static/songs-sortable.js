import { rocket } from 'datastar'

const sortableTbodies = ['songs-current', 'songs-waiting-removal', 'songs-not-recent']

// songs-sortable owns the client-side sort queue and the recent-toggle fetch
// lifecycle for the songs page. The server still renders the initial order and
// every mutation response; this component only reorders the existing rows and
// keeps them in the user's current sort after a response lands. The toggle
// request guard lives in songRecentChangeAction so it still runs when this
// module fails to load.
rocket('songs-sortable', {
	mode: 'light',
	props: ({ oneOf }) => ({
		sort: oneOf('added_desc', 'release_asc').default('added_desc'),
	}),
	setup: ({ cleanup, host, observeProps, props }) => {
		let sortQueue = Promise.resolve()
		let currentSort = props.sort

		const applySort = async (sortMode) => {
			const attr = sortMode === 'release_asc' ? 'data-order-release' : 'data-order-added'
			const jobs = []
			for (const tableId of ['songs-current', 'songs-waiting-removal']) {
				const tbody = host.querySelector('#' + tableId)
				if (!tbody || tbody.children.length <= 1) continue
				const parent = tbody.parentNode
				const next = tbody.nextSibling
				tbody.remove()
				jobs.push({ tbody, parent, next })
			}
			// Give Datastar and Rocket a microtask to drop the moved subtree's
			// transient observers before we reorder offline.
			try {
				await Promise.resolve()
				for (const { tbody, parent, next } of jobs) {
					const keyed = Array.from(tbody.children, (row) => [Number(row.getAttribute(attr)) || 0, row])
					keyed.sort((a, b) => a[0] - b[0])
					tbody.append(...keyed.map((entry) => entry[1]))
					parent.insertBefore(tbody, next)
				}
			} finally {
				// Never leave a table body detached if a reorder throws.
				for (const { tbody, parent, next } of jobs) {
					if (!tbody.isConnected) parent.insertBefore(tbody, next)
				}
			}
			const url = new URL(window.location)
			url.searchParams.set('playlist_sort', sortMode)
			window.history.replaceState({}, '', url)
		}

		const sortNow = (sortMode) => {
			sortQueue = sortQueue
				.then(
					() => applySort(sortMode),
					() => applySort(sortMode),
				)
				.catch((err) => {
					console.error('[songs-sortable] sort failed', err)
				})
			return sortQueue
		}

		observeProps(() => {
			currentSort = props.sort
			sortNow(currentSort)
		}, 'sort')

		const onFetch = (evt) => {
			const detail = evt.detail
			const el = detail?.el
			if (!el) return
			if (detail.type === 'finished' && el.tagName === 'TBODY' && sortableTbodies.includes(el.id)) {
				// The response was rendered with the sort captured when the
				// request started; reapply the user's current sort.
				sortNow(currentSort)
				return
			}
			if (detail.type === 'error' || detail.type === 'retries-failed') {
				const box = el.querySelector?.('input[data-song-id]:disabled')
				if (!box) return
				box.disabled = false
				if (box.dataset.wasChecked !== undefined) {
					box.checked = box.dataset.wasChecked === 'true'
				}
			}
		}
		document.addEventListener('datastar-fetch', onFetch)
		cleanup(() => document.removeEventListener('datastar-fetch', onFetch))
	},
})
