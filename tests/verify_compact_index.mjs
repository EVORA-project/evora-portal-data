// node tests/verify_compact_index.mjs <snapshot-dir> [report.json]
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, readdir, writeFile } from 'node:fs/promises';
import { createReference } from './compact-index/portal-reference.mjs';
import path from 'node:path';
import { gzipSync } from 'node:zlib';

const [directory, reportPath] = process.argv.slice(2);
if (!directory) throw Error('Expected snapshot directory');

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const measure = bytes => ({ raw: bytes.length, gzip: gzipSync(bytes, { level: 9 }).length });
const plain = value => JSON.parse(JSON.stringify(value));
const array = value => value == null ? [] : Array.isArray(value) ? value : [value];
const fields = 'id kind title description provider pathogen taxon publisher license category availability unit cost keywords additionalCategories accessUrl categoryFacet additionalCategoriesFacet providerFacet publisherFacet collectionFacet riskGroupFacet pathogenFacet taxonFacet keywordFacet refSku'.split(' ');

const full = await readFile(path.join(directory, 'catalogue.jsonld'));
const index = await readFile(path.join(directory, 'index.json'));
const idHash = hash(full);
const graph = JSON.parse(full)['@graph'];
const nodes = graph.filter(node => String(node?.['@type'] || '').startsWith('EVORAO:'));
const {schemaVersion, snapshot, resources} = JSON.parse(index);
assert.equal(schemaVersion, 'evora.portal.index/1');
assert.equal(snapshot.id, 'sha256:' + idHash);
assert.equal(path.basename(path.resolve(directory)), idHash);
assert.equal(snapshot.generatedAt, graph.find(node => node['@type'] === 'dcat:Catalog')['dcterms:modified']);
assert.equal(snapshot.resourceCount, nodes.length);
assert.equal(new Set(resources.map(doc => doc.id)).size, nodes.length);
assert.equal(resources.length, nodes.length);
assert.equal((await readdir(path.join(directory, 'entities'))).length, nodes.length);
const exportUrl = new URL(snapshot.exportUrl);
assert.equal(exportUrl.pathname, '/snapshots/' + idHash + '/catalogue.jsonld');

const current = createReference(), fullPortal = createReference();
const details = { raw: 0, gzip: 0 };
let evoraBrandingChecks = 0;
const fullExportRows = [];

function validLink(value) {
  assert.equal(typeof value, 'string');
  if (value) assert.ok(['https:', 'http:'].includes(new URL(value).protocol));
}

for (const [i, doc] of resources.entries()) {
  const node = nodes[i];
  const live = fullPortal.mapNodeToDoc(node, i);
  fullExportRows.push(live);
  for (const key of fields) {
    assert.deepEqual(doc[key], plain(live[key]), doc.id + ' / ' + key);
  }
  assert.equal(doc.id, node['@id']);
  assert.equal(doc.detail.snapshotId, snapshot.id);
  const file = hash(Buffer.from(doc.id, 'utf8')) + '.jsonld';
  assert.equal(doc.detail.url, new URL('entities/' + file, exportUrl).href);
  const data = await readFile(path.join(directory, 'entities', file));
  assert.deepEqual(JSON.parse(data), node, 'full detail / ' + doc.id);
  const size = measure(data);
  details.raw += size.raw;
  details.gzip += size.gzip;
  validLink(doc.accessUrl);
  for (const brand of [doc.providerBranding, doc.publisherBranding]) {
    assert.equal(typeof brand.name, 'string');
    validLink(brand.homepage);
  }
  assert.equal(doc.providerBranding.name, doc.providerFacet);
  assert.equal(doc.publisherBranding.name, doc.publisherFacet);
  for (const image of [...doc.providerBranding.logos, ...doc.publisherBranding.logos, ...doc.licenses.flatMap(l => l.logos)]) {
    assert.ok(image.contentUrl);
    validLink(image.contentUrl);
    assert.equal(typeof image.name, 'string');
    assert.equal(typeof image.format, 'string');
  }
  assert.equal(doc.licenseShort, live.licenseShort);
  assert.deepEqual(doc.licenses.map(l => ({title:l.title, note:l.attributionText, url:l.url, logos:l.logos})), plain(live.licenses));
  for (const license of doc.licenses) {
    assert.equal(license.scope, 'source metadata');
    assert.ok(doc.provenance.some(p => p.id === license.sourceId));
    validLink(license.url);
    assert.doesNotMatch(license.attributionText, /<[^>]+>/);
  }
  const sourceUrls = array(node['dcterms:source'] || node['dct:source']).map(s => typeof s === 'string' ? s : s['@id']);
  for (const p of doc.provenance) validLink(p.sourceUrl);
  for (const source of sourceUrls) assert.ok(doc.provenance.some(p => p.sourceUrl === source));
  for (const c of array(node['EVORAO:collection'])) {
    assert.ok(doc.provenance.some(p => p.id === (c['@id'] || '') && p.title === c['dcterms:title']));
  }
  if (doc.id.startsWith('urn:evorao:FAIRSHARING:7589:')) {
    assert.equal(doc.providerBranding.name, 'EVORA');
    assert.equal(doc.publisherBranding.name, 'FAIRsharing');
    assert.ok(doc.providerBranding.logos[0]?.contentUrl && doc.publisherBranding.logos[0]?.contentUrl);
    assert.notEqual(doc.providerBranding.logos[0].contentUrl, doc.publisherBranding.logos[0].contentUrl);
    assert.ok(doc.licenses.some(l => l.url === 'https://creativecommons.org/licenses/by-sa/4.0/' && l.sourceId === 'https://fairsharing.org/7589/'));
    evoraBrandingChecks++;
  }
}

await fullPortal.load(fullExportRows);
await current.load(resources);
const queries = ['', 'SARS antibody', 'SARS + antibody', 'SARS & antibody', 'SARS OR Zika', '"cell culture"',
  'SARS -antibody', 'SARS -"cell culture"', '-antibody', 'COVID-19', '017V-06422', 'Riboviria',
  'zzzz-no-such-catalogue-resource', 'EVORA', 'RDMkit'];
const ids = rows => Array.from(rows, doc => doc.id);
const facets = Object.keys(current.activeFilters);
assert.equal(facets.length, 9);
const queryCounts = {};
let facetMapChecks = 0, multiSelectChecks = 0, crossFacetChecks = 0;
for (const query of queries) {
  const before = await fullPortal.searchDocsByQuery(query), after = await current.searchDocsByQuery(query);
  assert.deepEqual(ids(after), ids(before), query);
  queryCounts[query] = after.length;
  const oldCounts = fullPortal.computeFacetCounts(before), newCounts = current.computeFacetCounts(after);
  function compareCounts(compactCounts, fullCounts) {
    for (const facet of facets) {
      assert.deepEqual([...compactCounts[facet]], [...fullCounts[facet]], query + ' / ' + facet);
      facetMapChecks++;
    }
  }
  function checkFilters(filters) {
    for (const [facet, values] of filters) {
      for (const value of values) for (const portal of [current, fullPortal]) portal.activeFilters[facet].add(value);
    }
    const compactRows = after.filter(current.matchesActiveFilters);
    const fullRows = before.filter(fullPortal.matchesActiveFilters);
    assert.deepEqual(ids(compactRows), ids(fullRows), query + ' / ' + JSON.stringify(filters));
    compareCounts(current.computeFacetCounts(compactRows), fullPortal.computeFacetCounts(fullRows));
    for (const [facet] of filters) for (const portal of [current, fullPortal]) portal.activeFilters[facet].clear();
  }
  compareCounts(newCounts, oldCounts);
  const selections = facets.map(facet => [facet, [...oldCounts[facet].keys()].slice(0, 2)])
    .filter(([, values]) => values.length);
  for (const [i, selection] of selections.entries()) {
    if (selection[1].length === 2) {
      checkFilters([selection]);
      multiSelectChecks++;
    }
    for (const other of selections.slice(i + 1)) {
      checkFilters([selection, other]);
      crossFacetChecks++;
    }
  }
}

const report = { schemaVersion, snapshot, portalReference: 'v1.6.0',
  bytes: { index: measure(index), catalogue: measure(full), detailsIndividually: details }, gzipLevel: 9,
  verified: { resources: resources.length, projectionFields: fields.length, fullDetails: resources.length, evoraBrandingChecks,
    queries: queryCounts, facetMapChecks, multiSelectChecks, crossFacetChecks } };
const reportText = JSON.stringify(report, null, 2);
if (reportPath) await writeFile(reportPath, reportText + '\n');
console.log(reportText);
