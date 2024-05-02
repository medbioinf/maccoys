docker-imgs:
	docker build -t medbioinf/maccoys-comet:latest -f docker/comet/Dockerfile .
	docker build -t medbioinf/maccoys:latest -f docker/maccoys/Dockerfile .
	docker build -t medbioinf/maccoys-py:latest -f docker/maccoys_py/Dockerfile .