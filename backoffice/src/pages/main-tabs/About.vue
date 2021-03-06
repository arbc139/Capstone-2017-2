<template>
<div class="root">
    <md-whiteframe class="root-paper" md-elevation="2">
      <div class="title-container">
        <h1>Welcome to GENAX</h1>
        <h4>GENAX uses the keywords you enter to dynamically find genes associated with your keywords.</h4>
        <span class="md-body-1">GENAX is a web-based server that extracts gene-gene interactions from the biomedical literature. GENAX provides several customizing options, including time, cut-off, weight of edge, and so on. Through dynamic collection, GENAX always provides knowledge extracted from the latest publications. Furthermore, all analysis results can be downloaded in .csv file format.</span>
        <span class="md-body-1">All instructions needed to run GENAX are available <a href="http://help.genax.tools">here</a> (or click on the document button in the right corner).</span>
        <span class="md-body-1">The scheme below illustrates the overall processing steps of GENAX.</span>
      </div>
      <md-whiteframe class="chart-container" md-elevation="2">
        <img src="/static/GENAX_Step.png" style="height: 80%; width: 80%; background-size: contain;">
      </md-whiteframe>
      <h4>Input</h4>
        <span class="md-body-1">GENAX makes a query from user-given keywords and time period. From the query, GENAX collects relevant articles from PubMed.</span>
        <h4>Paper Collect</h4>
        <span class="md-body-1">GENAX collects articles with a multi-thread collecting strategy, which allows for a dynamic literature retrieval feature. After all the target papers are collected, GENAX extracts MeSH terms from the papers, which are then converted to gene symbols according to pre-defined rules.</span>
        <h4>Network Building</h4>
        <span class="md-body-1">From the paper-gene relationship, GENAX creates edges of the gene network. Genes that are simultaneously referenced in the same paper are the co-occurring genes, and genes that appear alone in the papers are single-occurring genes. GENAX makes contingency tables for all co-occurring gene pairs and calculates the statistical significance of each co-occurring gene pair. All of the gene pairs become edges of the gene network, and statistical significance of the gene pair is based on the weight of the edge. GENAX then creates another network by adding single-occurring nodes to the co-occurring gene network.</span>
        <h4>Network Analysis</h4>
        <span class="md-body-1">After network building is complete, there are two networks generated. One is the co-occurring genes only network and the other is the co-occurring genes plus single-occurring genes network. With these two networks, GENAX calculates the centrality of each gene node. The user can obtain the gene scores from the centrality calculation. The user can also access the references from which the genes are collected.</span>
    </md-whiteframe>
</div>

</template>

<script>
export default {
  name: 'about',
};
</script>

<style scoped>
.root {
  overflow: auto;
  padding: 16px;
}

.root-paper {
  display: flex;
  flex-direction: column;
  height: fit-content;
  padding: 16px;
}

.title-container {
  display: flex;
  flex-direction: column;
  margin-bottom: 16px;
}

.chart-container {
  display: flex;
  flex-direction: row;
  padding: 16px;
  margin-bottom: 16px;
}

.chart-container:last-child {
  margin-bottom: 0;
}

.chart-instruction {
  display: flex;
  flex-direction: column;
  flex: 1;
}

.chart-title {
  margin-bottom: 8px;
}

.chart {
  margin-left: 16px;
  flex: 1;
}
</style>
