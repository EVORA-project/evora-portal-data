// EVORA Portal v1.6.0 (9bbb8fff), assets-no-min/base-builder.js
import { create, insert, search } from '@orama/orama';

export function createReference() {
    let db, allDocs;
    const metadataPool = new Map();
    const schema = {
      id: 'string',
      kind: 'string',
      title: 'string',
      description: 'string',
      provider: 'string',
      pathogen: 'string',
      taxon: 'string',
      publisher: 'string',
      license: 'string',
      category: 'string',
      availability: 'string',
      unit: 'string',
      cost: 'string',
      keywords: 'string'
    };
    const activeFilters = {
      category: new Set(),
      provider: new Set(),
      publisher: new Set(),
      additionalCategories: new Set(),
      taxon: new Set(),
      pathogen: new Set(),
      collection: new Set(),
      riskGroup: new Set(),
      keyword: new Set()
    };
    function stripHtml(str) {
      if (!str) return '';
      return String(str).replace(/<[^>]+>/g, '').trim();
    }

    function safeUrl(value) {
      if (value && typeof value === 'object') value = value['@id'];
      try {
        const url = new URL(String(value || ''));
        return ['http:', 'https:'].includes(url.protocol) ? url.href : '';
      } catch { return ''; }
    }

    function evoraValue(node, key) {
      return node?.[`EVORAO:${key}`] ?? node?.[`evorao:${key}`] ?? node?.[`https://w3id.org/evorao/${key}`];
    }

    function asArray(value) { return value == null ? [] : Array.isArray(value) ? value : [value]; }

    function internMetadata(value) {
      const key = JSON.stringify(value);
      if (!metadataPool.has(key)) metadataPool.set(key, value);
      return metadataPool.get(key);
    }

    function extractLogos(organization) {
      return asArray(organization).flatMap(org => asArray(evoraValue(org, 'logo'))).map(logo => ({
        contentUrl: safeUrl(evoraValue(logo, 'contentUrl') || logo?.['@id']),
        name: logo?.['foaf:name'] || '',
        format: evoraValue(logo, 'format') || ''
      })).filter(logo => logo.contentUrl);
    }

    function extractLicenses(node) {
      const result = [];
      for (const collection of asArray(evoraValue(node, 'collection'))) {
        const sources = [collection, ...asArray(evoraValue(collection, 'collectionDataProvider'))];
        for (const source of sources) for (const license of asArray(evoraValue(source, 'license') || source?.['dcterms:license'] || source?.['dct:license'])) {
          const title = typeof license === 'string' ? license : license?.['dcterms:title'] || license?.['dct:title'] || '';
          const note = stripHtml(evoraValue(license, 'licensingOrAttribution') || '');
          result.push({ title, note, url: safeUrl(evoraValue(license, 'resourceUrl') || license?.['@id'] || license), logos: extractLogos(license) });
        }
      }
      return internMetadata(result.filter((item, i) => result.findIndex(other => JSON.stringify(other) === JSON.stringify(item)) === i));
    }
    function getTypeLabel(node) {
      const t = node['@type'];
      if (Array.isArray(t)) {
        const nice = t.map(x => String(x).replace(/^EVORAO:/, '')).filter(Boolean);
        return nice.join(', ') || 'Entity';
      }
      if (typeof t === 'string') {
        return t.replace(/^EVORAO:/, '') || t;
      }
      return 'Entity';
    }

    function extractProvider(node) {
      const p = node['EVORAO:provider'];
      if (!p) return '';
      if (typeof p === 'string') return p;
      if (Array.isArray(p)) {
        return p
          .map((x) =>
            typeof x === 'string'
              ? x
              : x['foaf:name'] || x['dcterms:title'] || x['dct:title'] || ''
          )
          .filter(Boolean)
          .join(' | ');
      }
      return p['foaf:name'] || p['dcterms:title'] || p['dct:title'] || '';
    }

    function extractPublisher(node) {
      const pub =  node['dcterms:publisher'] || node['EVORAO:publisher'] || node['search:publisherName'];
      if (!pub) return '';
      if (typeof pub === 'string') return pub;
      if (Array.isArray(pub)) {
        return pub
          .map((x) =>
            typeof x === 'string'
              ? x
              : x['foaf:name'] || x['dcterms:title'] || x['dct:title'] || ''
          )
          .filter(Boolean)
          .join(' | ');
      }
      return pub['foaf:name'] || pub['dcterms:title'] || pub['dct:title'] || '';
    }

    function extractLicense(node) {
      return extractLicenses(node).map(license => [license.title, license.note].filter(Boolean).join(' – ')).join(' | ');
    }

    function extractPathogen(node) {
      const pid = node['EVORAO:pathogenIdentification'];
      if (!pid || typeof pid !== 'object') return '';
      const v = pid['EVORAO:pathogenName'];
      if (!v) return '';
      if (typeof v === 'string') return v;
      return v['dcterms:title'] || v['dct:title'] || '';
    }

    function extractTaxonFromPid(node) {
      const pid = node['EVORAO:pathogenIdentification'];
      if (!pid || typeof pid !== 'object') return '';
      const t = pid['EVORAO:taxon'];
      if (!t) return '';
      if (typeof t === 'string') return t;
      return t['dcterms:title'] || t['dct:title'] || '';
    }

    function extractTaxonHierarchy(node) {
      const raw = node['EVORAO:pathogenIdentification']?.['EVORAO:taxon']?.['EVORAO:lineage'] || node['search:taxon'];
      const arr = Array.isArray(raw) ? raw : raw ? [raw] : [];
      const labels = arr
        .map((x) => {
          if (typeof x === 'string') return x;
          if (x && typeof x === 'object') {
            return (
              x['dcterms:title'] ||
              x['dct:title'] ||
              x['rdfs:label'] ||
              x['@id'] ||
              ''
            );
          }
          return '';
        })
        .filter(Boolean)
        .filter((l) => l.toLowerCase() );
      return labels;
    }

    function extractCategory(node) {
      const sc = node['search:category'];
      if (Array.isArray(sc)) return sc.join(' | ');
      if (typeof sc === 'string') return sc;

      const c = node['EVORAO:category'];
      if (!c) return getTypeLabel(node);

      if (typeof c === 'string') return c;

      if (typeof c === 'object') {
        return (
          c['dcterms:title'] ||
          c['dct:title'] ||
          c['rdfs:label'] ||
          c['@id'] ||
          getTypeLabel(node)
        );
      }

      return getTypeLabel(node);
    }

    function extractAvailability(node) {
      return node['EVORAO:availability'] || '';
    }

    function extractUnitDefinition(node) {
      return node['EVORAO:unitDefinition'] || '';
    }

    function formatCostFromNode(node) {
      const amount = node['EVORAO:unitCost'];
      const note = node['EVORAO:unitCostNote'] || node['EVORAO:costNote'];
      const currency = node['EVORAO:unitCostCurrency'] || '';

      let label = '';
      if (typeof amount === 'number' && !Number.isNaN(amount)) {
        const formatted = amount.toFixed(2).replace(/\.00$/, '');
        label = currency ? `${currency} ${formatted}` : formatted;
      }
      return label || note || '';
    }

    function extractKeywordArray(node) {
      const k = node['dcat:keyword'];
      if (!k) return [];
      if (Array.isArray(k)) return k.map(String);
      return [String(k)];
    }

    function extractAccessUrl(node) {
      const url =
        node['EVORAO:accessPointURL'] ||
        node['EVORAO:accessPointUrl'] ||
        node['EVORAO:accessPoint'] ||
        '';
      return safeUrl(url);
    }
    function extractAdditionalCategories(node) {
      const cats = node['EVORAO:additionalCategory'];
      if (!cats) return [];
      const arr = Array.isArray(cats) ? cats : [cats];
      const out = [];
      for (const c of arr) {
        if (!c) continue;
        if (typeof c === 'string') {
          out.push(c);
        } else if (typeof c === 'object') {
          out.push(
            c['dcterms:title'] ||
              c['dct:title'] ||
              c['rdfs:label'] ||
              c['@id'] ||
              ''
          );
        }
      }
      return Array.from(new Set(out.filter(Boolean)));
    }

    // Map JSON-LD node → search document
    function mapNodeToDoc(node, idx) {
      const id = node['@id'] || `ent_${idx}`;

      const title =
        node['dcterms:title'] ||
        '';

      const description =
        node['dcterms:description'] ||
        '';

      const provider = extractProvider(node);
      const publisher = extractPublisher(node);
      const license = extractLicense(node);

      const pathogen = extractPathogen(node);
      const pathogenFacet = node['search:pathogenName'] || pathogen || '';

      const taxonHierarchy = extractTaxonHierarchy(node);
      const taxonLabel =
        node['search:taxonLabel'] ||
        (taxonHierarchy.length ? taxonHierarchy[0] : '') ||
        extractTaxonFromPid(node) ||
        '';

      const category = extractCategory(node);
      const additionalCategories = extractAdditionalCategories(node);
      const availability = extractAvailability(node);
      const unit = extractUnitDefinition(node);
      const cost = formatCostFromNode(node);
      const accessUrl = extractAccessUrl(node);

      const providerFacet = node['search:providerName'] || provider || '';
      const publisherFacet = node['search:publisherName'] || publisher || '';
      const collectionFacet = node['search:collectionName'] || '';
      const riskGroupFacet = node['search:riskGroupLabel'] || '';
      const keywordArray = extractKeywordArray(node);
      const refSku = node['EVORAO:refSku'] || '';

      const keywords = [
        ...keywordArray,
        providerFacet,
        publisherFacet,
        pathogenFacet,
        collectionFacet,
        riskGroupFacet,
        category,
        taxonLabel,
        taxonHierarchy.join(' '),
        refSku
      ]
        .filter(Boolean)
        .join(' ');

      const kind = getTypeLabel(node);

      return {
        id,
        kind,
        title,
        description,
        provider,
        pathogen: pathogenFacet,
        taxon: taxonLabel,
        publisher,
        license,
        licenseShort: extractLicenses(node).map(license => license.title || license.url).filter(Boolean).join(' | '),
        licenses: extractLicenses(node),
        providerLogos: internMetadata(extractLogos(evoraValue(node, 'provider'))),
        publisherLogos: internMetadata(extractLogos(node['dcterms:publisher'] || evoraValue(node, 'publisher'))).length
          ? internMetadata(extractLogos(node['dcterms:publisher'] || evoraValue(node, 'publisher')))
          : internMetadata(extractLicenses(node).flatMap(license => license.logos)),
        category,
        availability,
        unit,
        cost,
        keywords,
        additionalCategories,
        accessUrl,

        // Facet fields (not in schema, used only in JS)
        categoryFacet: category,
        additionalCategoriesFacet: additionalCategories,
        providerFacet,
        publisherFacet,
        collectionFacet,
        riskGroupFacet,
        pathogenFacet,
        taxonFacet: taxonHierarchy,
        keywordFacet: keywordArray,
        refSku
      };
    }
    function matchesActiveFilters(doc) {
      for (const [facet, set] of Object.entries(activeFilters)) {
        if (!set.size) continue;

        let values = [];
        switch (facet) {
          case 'category':
            values = doc.categoryFacet ? [doc.categoryFacet] : [];
            break;
          case 'additionalCategories':
            values = doc.additionalCategoriesFacet || [];
            break;
          case 'provider':
            values = doc.providerFacet ? [doc.providerFacet] : [];
            break;
          case 'publisher':
            values = doc.publisherFacet ? [doc.publisherFacet] : [];
            break;
          case 'collection':
            values = doc.collectionFacet ? [doc.collectionFacet] : [];
            break;
          case 'riskGroup':
            values = doc.riskGroupFacet ? [doc.riskGroupFacet] : [];
            break;
          case 'pathogen':
            values = doc.pathogenFacet ? [doc.pathogenFacet] : [];
            break;
          case 'taxon':
            values = doc.taxonFacet || [];
            break;
          case 'keyword':
            values = doc.keywordFacet || [];
            break;
        }

        if (!values || !values.length) return false;
        const hasOverlap = values.some((v) => set.has(String(v)));
        if (!hasOverlap) return false;
      }
      return true;
    }

    function computeFacetCounts(rows) {
      const counts = {
        category: new Map(),
        additionalCategories: new Map(),
        provider: new Map(),
        publisher: new Map(),
        taxon: new Map(),
        pathogen: new Map(),
        collection: new Map(),
        riskGroup: new Map(),
        keyword: new Map()
      };

      for (const doc of rows) {
        const add = (facet, value) => {
          if (!value) return;
          const map = counts[facet];
          if (!map) return;
          const v = String(value);
          map.set(v, (map.get(v) || 0) + 1);
        };

        add('category', doc.categoryFacet);
        (doc.additionalCategoriesFacet || []).forEach(v =>add('additionalCategories', v));
        add('provider', doc.providerFacet);
        add('publisher', doc.publisherFacet);
        add('collection', doc.collectionFacet);
        add('riskGroup', doc.riskGroupFacet);
        add('pathogen', doc.pathogenFacet);
        (doc.taxonFacet || []).forEach((t) => add('taxon', t));
        (doc.keywordFacet || []).forEach((k) => add('keyword', k));
      }

      return counts;
    }
    const SEARCHABLE_PROPERTIES = [
      'title',
      'description',
      'provider',
      'pathogen',
      'taxon',
      'publisher',
      'keywords'
    ];

    function normalizeSearchText(value) {
      return String(value || '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
    }

    function getCombinedSearchableText(doc) {
      return SEARCHABLE_PROPERTIES
        .map((prop) => String(doc[prop] || ''))
        .join(' ')
        .toLowerCase();
    }

    async function searchSingleTerm(term) {
      const result = await search(db, {
        term,
        limit: allDocs.length,
        properties: SEARCHABLE_PROPERTIES
      });

      return new Set(result.hits.map((h) => h.document.id));
    }

    async function searchByAndTerms(terms) {
      const cleanTerms = terms
        .map((term) => normalizeSearchText(term))
        .filter(Boolean);

      if (!cleanTerms.length) return allDocs;

      let matchingIds = null;

      for (const term of cleanTerms) {
        const ids = await searchSingleTerm(term);

        if (matchingIds === null) {
          matchingIds = ids;
        } else {
          matchingIds = new Set(
            [...matchingIds].filter((id) => ids.has(id))
          );
        }

        if (matchingIds.size === 0) break;
      }

      return allDocs.filter((doc) => matchingIds && matchingIds.has(doc.id));
    }

    async function searchByOrTerms(terms) {
      const cleanTerms = terms
        .map((term) => normalizeSearchText(term))
        .filter(Boolean);

      if (!cleanTerms.length) return allDocs;

      const unionIds = new Set();

      for (const term of cleanTerms) {
        const ids = await searchSingleTerm(term);
        for (const id of ids) {
          unionIds.add(id);
        }
      }

      return allDocs.filter((doc) => unionIds.has(doc.id));
    }

    async function searchDocsByQuery(rawQuery) {
      const query = rawQuery.trim();
      if (!query) return allDocs;

      // --------------------
      // Extract NOT terms
      // Only matches start-of-query "-" or whitespace + "-"
      // so COVID-19 stays intact
      // Supports:
      //   SARS -antibody
      //   SARS -"cell culture"
      // --------------------
      let positivePart = query;
      let negativeTerms = [];

      const negativeMatches = [
        ...query.matchAll(/(?:^|\s)-(?:"([^"]+)"|(\S+))/g)
      ];

      if (negativeMatches.length) {
        negativeTerms = negativeMatches
          .map((match) => normalizeSearchText(match[1] || match[2]))
          .filter(Boolean);

        positivePart = query
          .replace(/(?:^|\s)-(?:"([^"]+)"|(\S+))/g, ' ')
          .replace(/\s+/g, ' ')
          .trim();
      }

      if (!positivePart) {
        let results = allDocs;

        if (negativeTerms.length) {
          results = results.filter((doc) => {
            const text = getCombinedSearchableText(doc);
            return !negativeTerms.some((term) => text.includes(term));
          });
        }

        return results;
      }

      let results = [];

      // --------------------
      // Exact phrase mode
      // Example: "Antiviral therapy"
      // --------------------
      const phraseMatch = positivePart.match(/^"(.+)"$/);
      if (phraseMatch) {
        const phrase = normalizeSearchText(phraseMatch[1]);

        results = allDocs.filter((doc) =>
          getCombinedSearchableText(doc).includes(phrase)
        );
      }

      // --------------------
      // Explicit OR mode
      // Example: Antiviral OR therapy
      // --------------------
      else if (/\s+OR\s+/i.test(positivePart)) {
        const terms = positivePart.split(/\s+OR\s+/i);
        results = await searchByOrTerms(terms);
      }

      // --------------------
      // Explicit AND mode
      // Example: Antiviral + therapy
      // Example: Antiviral & therapy
      // --------------------
      else if (positivePart.includes('+') || positivePart.includes('&')) {
        const terms = positivePart.split(/[&+]/);
        results = await searchByAndTerms(terms);
      }

      // --------------------
      // Default = implicit AND
      // Example: Antiviral therapy
      // --------------------
      else {
        const terms = positivePart.split(/\s+/);
        results = await searchByAndTerms(terms);
      }

      // --------------------
      // Apply NOT exclusions
      // --------------------
      if (negativeTerms.length) {
        results = results.filter((doc) => {
          const text = getCombinedSearchableText(doc);
          return !negativeTerms.some((term) => text.includes(term));
        });
      }

      return results;
    }
    return { mapNodeToDoc, computeFacetCounts, matchesActiveFilters, activeFilters, searchDocsByQuery,
      async load(rows) {
        db = await create({ schema });
        allDocs = rows;
        for (const row of rows) await insert(db, row);
      }
    };
}
