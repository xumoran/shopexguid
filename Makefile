OBJ:=guid

all: ${OBJ}

${OBJ}: *.go
	go build

start: ${OBJ}
	./${OBJ} start

readme: ${OBJ}
	cat intro.md > README.md
	echo '```' >> README.md
	./${OBJ} help 2>> README.md
	echo '```' >> README.md
	echo "\n-------------------------\n" >>  README.md
	echo '```' >> README.md
	./${OBJ} help start 2>> README.md
	echo '```' >> README.md
	echo "\n-------------------------\n" >>  README.md
	echo '```' >> README.md
	./${OBJ} help import 2>> README.md
	echo '```' >> README.md
	echo "\n-------------------------\n" >>  README.md
	echo '```' >> README.md
	./${OBJ} help top 2>> README.md
	echo '```' >> README.md
	echo "\n-------------------------\n" >>  README.md
	echo '```' >> README.md
	./${OBJ} help clear-redis 2>> README.md
	echo '```' >> README.md
	echo "\n-------------------------\n" >>  README.md
	echo '```' >> README.md
	./${OBJ} help has 2>> README.md
	echo '```' >> README.md