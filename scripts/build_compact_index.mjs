// node scripts/build_compact_index.mjs [merged.jsonld] [preview-root] [hosting-origin] [same-run portal source for search annotations]
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';

const [input = 'data/portal/evora_portal_all.jsonld', output = '.compact-index-preview',
  origin = 'https://data.evora-project.eu', projectionSource] = process.argv.slice(2);
const schemaVersion = 'evora.portal.index/1';
const array = value => value == null ? [] : Array.isArray(value) ? value : [value];
const field = (obj, key) => obj?.[`EVORAO:${key}`] ?? obj?.[`evorao:${key}`] ?? obj?.[`https://w3id.org/evorao/${key}`];
const title = obj => obj?.['dcterms:title'] || obj?.['dct:title'] || '';
const label = obj => typeof obj === 'string' ? obj : title(obj) || obj?.['rdfs:label'] || obj?.['@id'] || '';
const orgName = obj => typeof obj === 'string' ? obj : obj?.['foaf:name'] || title(obj);
const names = orgs => array(orgs).map(orgName).filter(Boolean).join(' | ');
const plain = value => String(value || '').replace(/<[^>]+>/g, '').trim();
const unique = values => [...new Set(values)];
const homepage = obj => url(field(obj, 'homepage') || obj?.['foaf:homepage']);

function url(value) {
  try {
    const parsed = new URL(typeof value === 'object' ? value?.['@id'] : value);
    if (['http:', 'https:'].includes(parsed.protocol)) return parsed.href;
  } catch { /* Missing or unsupported source link. */ }
  return '';
}

function logos(sources) {
  return array(sources).flatMap(source => array(field(source, 'logo'))).map(image => ({
    contentUrl: url(field(image, 'contentUrl')), name: plain(image?.['foaf:name']),
    format: field(image, 'format') || ''
  })).filter(image => image.contentUrl);
}

function branding(orgs, name) {
  const sites = unique(array(orgs).map(homepage).filter(Boolean));
  return { name, homepage: sites.length === 1 ? sites[0] : '', logos: logos(orgs) };
}

function sourceMetadata(node) {
  const licenses = [], provenance = [], dataProviders = [];
  const sources = unique(array(node['dcterms:source'] || node['dct:source']).map(url).filter(Boolean));
  const collections = array(field(node, 'collection'));
  for (const collection of collections) {
    const id = typeof collection === 'string' ? collection : collection['@id'] || '';
    const providers = array(field(collection, 'collectionDataProvider'));
    dataProviders.push(...providers);
    for (const source of [collection, ...providers]) {
      const terms = field(source, 'license') || source?.['dcterms:license'] || source?.['dct:license'];
      for (const license of array(terms)) {
        licenses.push({
          title: plain(typeof license === 'string' ? license : title(license)),
          url: url(field(license, 'resourceUrl') || license?.['@id'] || license),
          attributionText: plain(field(license, 'licensingOrAttribution')),
          logos: logos(license), scope: 'source metadata', sourceId: id
        });
      }
    }
    // A general access point may be an application form, not a provenance link.
    for (const sourceUrl of sources.length ? sources : ['']) {
      provenance.push({ id, title: plain(label(collection)), sourceUrl });
    }
  }
  if (!collections.length) for (const sourceUrl of sources) provenance.push({ id: '', title: '', sourceUrl });
  return { licenses, provenance, dataProviders };
}

function project(node, detail) {
  const id = node['@id'];
  const kind = array(node['@type']).map(type => String(type).replace(/^EVORAO:/, '')).filter(Boolean).join(', ') || 'Entity';
  const providerOrg = field(node, 'provider');
  const publisherOrg = node['dcterms:publisher'] || field(node, 'publisher');
  const provider = names(providerOrg), publisher = names(publisherOrg || node['search:publisherName']);
  const providerFacet = node['search:providerName'] || provider;
  const publisherFacet = node['search:publisherName'] || publisher;
  const { licenses, provenance, dataProviders } = sourceMetadata(node);
  const license = unique(licenses.map(item => [item.title, item.attributionText].filter(Boolean).join(' – '))).join(' | ');
  const licenseShort = unique(licenses.map(item => item.title || item.url).filter(Boolean)).join(' | ');
  // Older FAIRsharing records identify the publisher via its collection data provider.
  const credited = dataProviders.filter(source => [publisher, publisherFacet].includes(orgName(source)));
  const publisherBranding = branding(publisherOrg || credited, publisherFacet);
  if (!publisherBranding.homepage) publisherBranding.homepage = branding(credited, publisherFacet).homepage;
  // Use attribution artwork only when the credited data provider matches this publisher.
  if (!publisherBranding.logos.length) {
    const artwork = credited.flatMap(source =>
      array(field(source, 'license') || source['dcterms:license'] || source['dct:license']).flatMap(logos));
    publisherBranding.logos = [...new Map(artwork.map(image => [image.contentUrl, image])).values()];
  }
  const identification = field(node, 'pathogenIdentification');
  const pathogenName = field(identification, 'pathogenName');
  const pathogen = node['search:pathogenName'] || (typeof pathogenName === 'string' ? pathogenName : title(pathogenName));
  const taxonNode = field(identification, 'taxon');
  const taxonFacet = array(field(taxonNode, 'lineage') || node['search:taxon']).map(label).filter(Boolean);
  const taxon = node['search:taxonLabel'] || taxonFacet[0] || (typeof taxonNode === 'string' ? taxonNode : title(taxonNode));
  const searchCategory = node['search:category'];
  const category = Array.isArray(searchCategory) ? searchCategory.join(' | ') :
    typeof searchCategory === 'string' ? searchCategory : label(node['dcat:theme'] || field(node, 'category')) || kind;
  const additionalCategories = unique(array(field(node, 'additionalCategory')).map(label).filter(Boolean));
  const collectionFacet = node['search:collectionName'] || '', riskGroupFacet = node['search:riskGroupLabel'] || '';
  const keywordFacet = array(node['dcat:keyword']).map(String), refSku = field(node, 'refSku') || '';
  const keywords = [...keywordFacet, providerFacet, publisherFacet, pathogen, collectionFacet, riskGroupFacet,
    category, taxon, taxonFacet.join(' '), refSku].filter(Boolean).join(' ');
  const amount = field(node, 'unitCost'), currency = field(node, 'unitCostCurrency');
  const cost = typeof amount === 'number' ? [currency, amount.toFixed(2).replace(/\.00$/, '')].filter(Boolean).join(' ') :
    field(node, 'unitCostNote') || field(node, 'costNote') || '';
  return {
    id, kind, title: node['dcterms:title'] || '', description: node['dcterms:description'] || '',
    provider, pathogen, taxon, publisher, license, category, availability: field(node, 'availability') || '',
    unit: field(node, 'unitDefinition') || '', cost, keywords, additionalCategories,
    accessUrl: url(field(node, 'accessPointURL') || field(node, 'accessPointUrl') || field(node, 'accessPoint')),
    categoryFacet: category, additionalCategoriesFacet: additionalCategories, providerFacet, publisherFacet,
    collectionFacet, riskGroupFacet, pathogenFacet: pathogen, taxonFacet, keywordFacet, refSku,
    providerBranding: branding(providerOrg, providerFacet), publisherBranding,
    licenses, licenseShort, provenance, detail
  };
}

const bytes = await readFile(input);
const hash = createHash('sha256').update(bytes).digest('hex');
const graph = JSON.parse(bytes)['@graph'];
const catalogue = graph.find(node => node?.['@id'] === 'https://portal.evora-project.eu/catalogue');
const generatedAt = catalogue?.['dcterms:modified'];
if (!generatedAt || !Number.isFinite(Date.parse(generatedAt))) throw Error('Missing catalogue timestamp');
const base = new URL(`/snapshots/${hash}/`, origin);
if (!url(base.href)) throw Error('Expected HTTP(S) hosting origin');
const isResource = node => String(node?.['@type'] || '').startsWith('EVORAO:');
const nodes = graph.filter(isResource);
let sourceNodes;
if (projectionSource) {
  const sourceGraph = JSON.parse(await readFile(projectionSource))['@graph'];
  sourceNodes = sourceGraph.filter(isResource);
  if (sourceNodes.length !== nodes.length || nodes.some((node, i) => node['@id'] !== sourceNodes[i]['@id'])) {
    throw Error('Canonical and portal resource IDs or ordering differ');
  }
  const sourceCatalogue = sourceGraph.find(node => node?.['@id'] === catalogue['@id']);
  if (sourceCatalogue?.['dcterms:modified'] !== generatedAt) throw Error('Source snapshot timestamps differ');
}
const ids = new Set();
for (const node of nodes) {
  const id = node['@id'];
  if (typeof id !== 'string' || !id.trim() || ids.has(id)) throw Error('Missing or duplicate resource ID: ' + id);
  ids.add(id);
}
const members = catalogue['dcat:dataset']?.map(member => member['@id']);
if (members?.length !== ids.size || new Set(members).size !== ids.size || !members.every(id => ids.has(id))) {
  throw Error('Catalogue membership differs from indexed resources');
}
const snapshot = { id: 'sha256:' + hash, generatedAt, exportUrl: new URL('catalogue.jsonld', base).href, resourceCount: nodes.length };
const parent = path.join(output, 'snapshots');
await mkdir(parent, { recursive: true });
const stage = await mkdtemp(path.join(parent, '.building-'));
try {
  await mkdir(path.join(stage, 'entities'));
  const resources = [];
  for (const [i, node] of nodes.entries()) {
    // A digest of the UTF-8 ID is a portable single path segment, even for long HTTP IRIs.
    const file = createHash('sha256').update(node['@id']).digest('hex') + '.jsonld';
    const detail = { snapshotId: snapshot.id, url: new URL('entities/' + file, base).href };
    // Only derived search annotations come from the companion source. The raw
    // detail and all semantic properties come from the exact hashed export.
    const annotations = sourceNodes
      ? Object.fromEntries(Object.entries(sourceNodes[i]).filter(([key]) => key.startsWith('search:'))) : {};
    const doc = project({ ...node, ...annotations }, detail);
    for (const [key, value] of Object.entries(doc)) {
      if (['detail', 'provenance', 'licenses', 'providerBranding', 'publisherBranding'].includes(key)) continue;
      const valid = ['additionalCategories', 'additionalCategoriesFacet', 'taxonFacet', 'keywordFacet'].includes(key)
        ? Array.isArray(value) && value.every(item => typeof item === 'string') : typeof value === 'string';
      if (!valid) throw Error(`Invalid ${key} on ${doc.id}`);
    }
    resources.push(doc);
    await writeFile(path.join(stage, 'entities', file), JSON.stringify(node), { flag: 'wx' });
  }
  await writeFile(path.join(stage, 'catalogue.jsonld'), bytes);
  await writeFile(path.join(stage, 'index.json'), JSON.stringify({ schemaVersion, snapshot, resources }));
  await rename(stage, path.join(parent, hash));
  console.log(path.resolve(parent, hash));
} finally {
  await rm(stage, { recursive: true, force: true });
}
