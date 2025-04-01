FROM amacneil/dbmate

COPY db /db

ENTRYPOINT ["dbmate"]
CMD ["--wait", "--no-dump-schema", "up"]
