.PHONY: serve test clean

serve:
	python3 server.py --port 50059

test:
	@for t in test_core test_storage test_mcp_e2e test_agent_consumption; do \
		printf "%-26s " "$$t"; python3 tests/$$t.py 2>&1 | tail -1; \
	done

clean:
	rm -f news.db news.db-wal news.db-shm
