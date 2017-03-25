README
------

1) Source code for Search Engine Implementation can be found in
/src/iu/pti/hbaseapp/clueweb09/SearchEngineTester.java

2) Output is located in /output folder

search_keyword_snapshot.txt: Output for command below
hadoop jar cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.SearchEngineTester search-keyword snapshot > /root/MoocHomeworks/HBaseInvertedIndexing/output/search_keyword_snapshot.txt

get_page_snapshot: Output for command below
hadoop jar cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.SearchEngineTester get-page-snapshot 00000113548 | grep snapshot > /root/MoocHomeworks/HBaseInvertedIndexing/output/get_page_snapshot.txt