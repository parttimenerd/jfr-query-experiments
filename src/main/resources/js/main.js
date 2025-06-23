
let previousQueries = new Set();
let autoQueryInterval = null;
let lastExecutedQuery = "";
let queryInput, highlightDiv;
let autocompleteSuggestions;
let currentSuggestions = [];
let selectedSuggestionIndex = -1;

// Add these variables at the top with other globals
let selectedRows = new Set();
let isSelectMode = false;
let lastSelectedRowIndex = -1; // Track the last selected row for range selection

// Add a cache for SHOW FIELDS results
const showFieldsCache = new Map();

// Centralized vocabulary definitions - single source of truth
const queryVocabulary = {
    keywords: [
        'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT',
        'COLUMN', 'CAPTION', 'FORMAT', 'AS', 'AND', 'ASC', 'DESC',
        'SHOW EVENTS', 'SHOW FIELDS'
    ],

    functions: [
        'AVG', 'COUNT', 'DIFF', 'FIRST', 'LAST', 'LAST_BATCH',
        'LIST', 'MAX', 'MEDIAN', 'MIN', 'P90', 'P95', 'P99', 'P999',
        'STDEV', 'SUM', 'UNIQUE'
    ],

    formatProperties: [
        'cell-height:', 'missing:whitespace', 'normalized',
        'truncate-beginning', 'truncate-end', 'missing:null-bootstrap'
    ],

    // Will be populated dynamically from SHOW EVENTS, fallback to common ones
    availableEventTypes: [
        'GarbageCollection', 'ExecutionSample', 'ObjectAllocationSample',
        'JavaMonitorEnter', 'JavaMonitorWait', 'ThreadPark', 'ThreadSleep',
        'SocketRead', 'SocketWrite', 'FileRead', 'FileWrite',
        'ClassLoad', 'Compilation', 'Deoptimization',
        'jdk.GarbageCollection', 'jdk.ExecutionSample', 'jdk.ObjectAllocationSample',
        'jdk.JavaMonitorEnter', 'jdk.ThreadPark', 'jdk.ClassLoad',
        'SystemProcess', 'CPULoad', 'GCHeapSummary', 'ActiveRecording'
    ],

    commonFields: [
        'startTime', 'duration', 'eventThread', 'stackTrace',
        'stackTrace.topFrame', 'stackTrace.topApplicationFrame',
        'eventType.label', 'gcId', 'objectClass', 'weight',
        'pid', 'path', 'bytesRead', 'bytesWritten', 'host'
    ],

    // Mix of strings and objects for sample queries
    sampleQueries: [
        // Simple command strings
        "grammar",
        "views",
        "SHOW EVENTS",
        "SHOW FIELDS jdk.GarbageCollection",

        // Simple query strings
        "SELECT * FROM CPULoad",
        "SELECT * FROM GarbageCollection",
        "SELECT * FROM ExecutionSample",
        "SELECT * FROM ObjectAllocationSample",
        "SELECT startTime FROM GarbageCollection",
        "SELECT COUNT(*) FROM GarbageCollection",

        // Objects with descriptions
        {
            text: "SELECT stackTrace.topFrame AS T, SUM(weight) FROM ObjectAllocationSample GROUP BY T",
            type: 'sample',
            description: "Allocation pressure by method"
        },
        {
            text: "SELECT gcId, duration FROM GarbageCollection ORDER BY duration DESC",
            type: 'sample',
            description: "GC events by duration"
        }
        // View queries will be added dynamically by loadViewQueries()
    ]
};

// Syntax highlighting functions using centralized vocabulary
function createSyntaxHighlighter() {
    const originalQueryInput = document.getElementById('queryInput');
    const container = originalQueryInput.parentNode;

    const wrapper = document.createElement('div');
    wrapper.className = 'syntax-highlighter-wrapper';

    const highlightDiv = document.createElement('div');
    highlightDiv.id = 'highlightDiv';

    originalQueryInput.className = 'highlighted';

    container.replaceChild(wrapper, originalQueryInput);
    wrapper.appendChild(highlightDiv);
    wrapper.appendChild(originalQueryInput);

    return { queryInput: originalQueryInput, highlightDiv };
}

function highlightSyntax(text) {
    // Use centralized vocabulary
    const keywords = queryVocabulary.keywords;
    const functions = queryVocabulary.functions;

    text = text.replace(/[<>&]/g, function(match) {
        return {'<': '&lt;', '>': '&gt;', '&': '&amp;'}[match];
    });

    text = text.replace(/'([^']*)'/g, '<span class="sql-string">\'$1\'</span>');
    text = text.replace(/\b\d+\b/g, '<span class="sql-number">$&</span>');

    keywords.forEach(keyword => {
        const regex = new RegExp(`\\b${keyword.replace(/\s+/g, '\\s+')}\\b`, 'gi');
        text = text.replace(regex, '<span class="sql-keyword">$&</span>');
    });

    functions.forEach(func => {
        const regex = new RegExp(`\\b${func}\\b(?=\\s*\\()`, 'gi');
        text = text.replace(regex, '<span class="sql-function">$&</span>');
    });

    text = text.replace(/(?<!<[^>]*)\b=\b(?![^<]*>)/g, '<span class="sql-operator">=</span>');
    text = text.replace(/(?<!<[^>]*),(?![^<]*>)/g, '<span class="sql-operator">,</span>');
    text = text.replace(/(?<!<[^>]*)\((?![^<]*>)/g, '<span class="sql-operator">(</span>');
    text = text.replace(/(?<!<[^>]*)\)(?![^<]*>)/g, '<span class="sql-operator">)</span>');

    return text;
}

function updateHighlighting() {
    if (highlightDiv && queryInput) {
        const text = queryInput.value;
        const highlighted = highlightSyntax(text);
        highlightDiv.innerHTML = highlighted;
    }
}

// Row selection functions
function updateSelectionUI() {
    const countEl = document.querySelector('.selection-count');
    
    if (countEl) {
        if (selectedRows.size > 0) {
            countEl.textContent = `${selectedRows.size} rows selected`;
            countEl.style.color = '#1976d2';
            countEl.style.fontWeight = 'bold';
        } else {
            countEl.textContent = '0 rows selected';
            countEl.style.color = '#666';
            countEl.style.fontWeight = 'normal';
        }
    }
    
    // Auto-update graph when selection changes
    autoUpdateGraph();
}

// Add the missing autoUpdateGraph function
function autoUpdateGraph() {
    if (!isSelectMode || !graphData) {
        return;
    }
    
    if (selectedRows.size === 0) {
        // No rows selected - show full dataset
        const showGraphCheckbox = document.getElementById('showGraph');
        if (showGraphCheckbox && showGraphCheckbox.checked) {
            // Show the full dataset graph
            if (graphContainer) {
                graphContainer.style.display = 'block';
                createGraph(graphData);
            }
        } else {
            // Hide graph container
            if (graphContainer) {
                graphContainer.style.display = 'none';
            }
        }
    } else {
        // Rows are selected - show graph with filtered data
        const filteredData = {
            ...graphData,
            data: graphData.data.filter((row, index) => selectedRows.has(index))
        };
        
        if (graphContainer) {
            graphContainer.style.display = 'block';
            createGraphFromSelection(filteredData);
            
            // Also enable the show graph checkbox
            const showGraphCheckbox = document.getElementById('showGraph');
            if (showGraphCheckbox) {
                showGraphCheckbox.checked = true;
            }
        }
        
        console.log(`Auto-updated graph with ${selectedRows.size} selected rows`);
    }
}

// Function to create graph from selection
function createGraphFromSelection(filteredData) {
    if (!filteredData || !filteredData.numericColumns.length) {
        showGraphStatus('No numeric columns found in selected rows');
        return;
    }
    
    if (filteredData.timeColumnIndex === -1) {
        showGraphStatus('No time column found in selected rows');
        return;
    }
    
    // Use the same logic as createGraph but with filtered data
    createGraph(filteredData);
    
    // Update the graph title or add a note about selection
    if (graphChart) {
        graphChart.options.plugins.title = {
            display: true,
            text: `Selected Data (${filteredData.data.length} rows)`
        };
        graphChart.update();
    }
}

function toggleSelectMode() {
    isSelectMode = !isSelectMode;
    const btn = document.getElementById('selectModeBtn');
    const actionBtns = document.querySelectorAll('.select-action-btn, .selection-count, .selection-hint');
    const selectColumns = document.querySelectorAll('.select-column');
    const table = document.querySelector('.selectable-table');
    
    if (isSelectMode) {
        btn.textContent = 'Disable Row Selection';
        btn.classList.add('active');
        actionBtns.forEach(el => el.style.display = 'inline-block');
        selectColumns.forEach(el => el.style.display = 'table-cell');
        if (table) table.classList.add('selection-mode');
        
        // Show instruction about auto-graphing
        const hint = document.querySelector('.selection-hint');
        if (hint) {
            hint.innerHTML = '💡 Hold Shift and click to select a range • Graph updates automatically';
        }
    } else {
        btn.textContent = 'Enable Row Selection';
        btn.classList.remove('active');
        actionBtns.forEach(el => el.style.display = 'none');
        selectColumns.forEach(el => el.style.display = 'none');
        if (table) table.classList.remove('selection-mode');
        clearSelection();
        
        // Hide graph when exiting selection mode
        if (graphContainer && graphChart) {
            // Don't completely hide the container, but clear the selection-based graph
            if (selectedRows.size === 0) {
                const showGraphCheckbox = document.getElementById('showGraph');
                if (showGraphCheckbox && showGraphCheckbox.checked) {
                    // Show the full dataset graph instead
                    createGraph(graphData);
                } else {
                    graphContainer.style.display = 'none';
                }
            }
        }
    }
}

// Enhanced toggleRowSelection to support shift-click range selection
function toggleRowSelection(rowIndex, event) {
    if (!isSelectMode) return;
    
    const row = document.querySelector(`tr[data-row-index="${rowIndex}"]`);
    const checkbox = row.querySelector('input[type="checkbox"]');
    
    // Check if shift key is held down and we have a previous selection
    if (event && event.shiftKey && lastSelectedRowIndex !== -1) {
        // Range selection
        const startIndex = Math.min(lastSelectedRowIndex, rowIndex);
        const endIndex = Math.max(lastSelectedRowIndex, rowIndex);
    
        // Select all rows in the range
        for (let i = startIndex; i <= endIndex; i++) {
            const rangeRow = document.querySelector(`tr[data-row-index="${i}"]`);
            const rangeCheckbox = rangeRow?.querySelector('input[type="checkbox"]');
            
            if (rangeRow && rangeCheckbox) {
                selectedRows.add(i);
                rangeRow.classList.add('selected-row');
                rangeCheckbox.checked = true;
            }
        }
        
        // Don't update lastSelectedRowIndex for range selection
        // Keep it as the anchor point for potential future range selections
    } else {
        // Single row selection (original logic)
        if (selectedRows.has(rowIndex)) {
            selectedRows.delete(rowIndex);
            row.classList.remove('selected-row');
            checkbox.checked = false;
            // If we're deselecting the last selected row, clear the anchor
            if (lastSelectedRowIndex === rowIndex) {
                lastSelectedRowIndex = -1;
            }
        } else {
            selectedRows.add(rowIndex);
            row.classList.add('selected-row');
            checkbox.checked = true;
            // Update the anchor point for future range selections
            lastSelectedRowIndex = rowIndex;
        }
    }
    
    updateSelectionUI(); // This now calls autoUpdateGraph()
}

// Enhanced clearSelection to reset the anchor point
function clearSelection() {
    selectedRows.clear();
    lastSelectedRowIndex = -1; // Reset anchor point
    
    document.querySelectorAll('.selected-row').forEach(row => {
        row.classList.remove('selected-row');
        const checkbox = row.querySelector('input[type="checkbox"]');
        if (checkbox) checkbox.checked = false;
    });
    
    updateSelectionUI(); // This now calls autoUpdateGraph()
    const selectAllCheckbox = document.getElementById('selectAllCheckbox');
    if (selectAllCheckbox) selectAllCheckbox.checked = false;
}

// Enhanced selectAllRows to set proper anchor point
function selectAllRows() {
    const rows = document.querySelectorAll('tr[data-row-index]');
    rows.forEach(row => {
        const rowIndex = parseInt(row.dataset.rowIndex);
        selectedRows.add(rowIndex);
        row.classList.add('selected-row');
        const checkbox = row.querySelector('input[type="checkbox"]');
        if (checkbox) checkbox.checked = true;
    });
    
    // Set anchor to the last row for potential future operations
    if (rows.length > 0) {
        lastSelectedRowIndex = parseInt(rows[rows.length - 1].dataset.rowIndex);
    }
    
    updateSelectionUI(); // This now calls autoUpdateGraph()
    const selectAllCheckbox = document.getElementById('selectAllCheckbox');
    if (selectAllCheckbox) selectAllCheckbox.checked = true;
}

// Function to handle individual row checkbox changes
function handleRowCheckbox(rowIndex, event) {
    // Stop event propagation to prevent triggering row click
    event.stopPropagation();
    
    // Toggle the row selection
    toggleRowSelection(rowIndex, event);
}

// Function to handle select all checkbox
function toggleSelectAll() {
    const selectAllCheckbox = document.getElementById('selectAllCheckbox');
    if (selectAllCheckbox.checked) {
        selectAllRows();
    } else {
        clearSelection();
    }
}

// Function to detect if output is tabular data
function isTabularData(data) {
    const lines = data.split('\n');
    if (lines.length < 3) return false;

    // Look for a header separator line with dashes
    for (let i = 1; i < Math.min(3, lines.length); i++) {
        const line = lines[i].trim();
        if (line.match(/^-+(\s+-+)+$/)) {
            return true;
        }
    }
    return false;
}

// Function to detect if output is SHOW EVENTS format
function isShowEventsData(data) {
    return data.includes('Event Types (number of events):') ||
            (data.includes('ActiveRecording') && data.includes('CPULoad') && data.includes('GarbageCollection'));
}

// Function to detect if output is SHOW FIELDS format
function isShowFieldsData(data) {
    // Look for field definitions with type and name patterns
    return data.match(/^\s*(long|int|String|Thread|boolean|float|double|byte|short|char)\s+\w+/m) ||
            data.includes('Event types with a') ||
            data.includes(':') && data.match(/^\s*\w+\s+\w+$/m);
}

// Function to parse SHOW FIELDS output into formatted HTML
function parseShowFieldsData(data) {
    const lines = data.split('\n');
    let html = '';
    let inEventSection = false;
    let currentEventName = '';
    let fields = [];
    let relatedEvents = [];

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();

        if (!line) continue;

        // Check if this is an event name (ends with colon)
        if (line.endsWith(':')) {
            // Process previous event if exists
            if (currentEventName && fields.length > 0) {
                html += formatEventFields(currentEventName, fields);
                fields = [];
            }

            currentEventName = line.slice(0, -1); // Remove colon
            inEventSection = false;
            continue;
        }

        // Check if this is a section header for related events
        if (line.includes('Event types with a') && line.includes('relation:')) {
            if (currentEventName && fields.length > 0) {
                html += formatEventFields(currentEventName, fields);
                fields = [];
            }
            html += `<div class="show-fields-section">
                <h3>${escapeHtml(line)}</h3>
            </div>`;
            inEventSection = true;
            continue;
        }

        // If we're in the related events section
        if (inEventSection) {
            // Parse multiple event names from the line
            const eventNames = line.split(/\s+/).filter(name => name.trim() && !name.match(/^\s*$/));
            relatedEvents.push(...eventNames);
            continue;
        }

        // Parse field definition (type fieldName)
        const fieldMatch = line.match(/^\s*(long|int|String|Thread|boolean|float|double|byte|short|char|[\w.]+)\s+(\w+)/);
        if (fieldMatch) {
            fields.push({
                type: fieldMatch[1],
                name: fieldMatch[2]
            });
        }
    }

    // Process the last event
    if (currentEventName && fields.length > 0) {
        html += formatEventFields(currentEventName, fields);
    }

    // Add related events section if we have any
    if (relatedEvents.length > 0) {
        html += formatRelatedEvents(relatedEvents);
    }

    return html;
}

// Function to format event fields as a table
function formatEventFields(eventName, fields) {
    let html = `<div class="show-fields-event">
        <h2>${escapeHtml(eventName)}</h2>
        <table class="jfr-table sortable-table show-fields-table">
            <thead>
                <tr>
                    <th style="text-align: left" data-sort="string">Field Name <span class="sort-indicator"></span></th>
                    <th style="text-align: left" data-sort="string">Type <span class="sort-indicator"></span></th>
                </tr>
            </thead>
            <tbody>`;

    for (const field of fields) {
        html += `<tr>
            <td style="text-align: left" data-value="${escapeHtml(field.name)}">${escapeHtml(field.name)}</td>
            <td style="text-align: left" data-value="${escapeHtml(field.type)}"><span class="field-type">${escapeHtml(field.type)}</span></td>
        </tr>`;
    }

    html += `    </tbody>
        </table>
    </div>`;

    return html;
}

// Function to format related events section
function formatRelatedEvents(relatedEvents) {
    // Group events by prefix for better organization
    const grouped = {};
    relatedEvents.forEach(event => {
        const prefix = event.includes('.') ? event.split('.')[0] : 'other';
        if (!grouped[prefix]) grouped[prefix] = [];
        grouped[prefix].push(event);
    });

    let html = `<div class="show-fields-related">
        <table class="jfr-table sortable-table show-fields-related-table">
            <thead>
                <tr>
                    <th style="text-align: left" data-sort="string">Related Event Types <span class="sort-indicator"></span></th>
                </tr>
            </thead>
            <tbody>`;

    // Sort events alphabetically
    relatedEvents.sort().forEach(event => {
        html += `<tr>
            <td style="text-align: left" data-value="${escapeHtml(event)}">${escapeHtml(event)}</td>
        </tr>`;
    });

    html += `    </tbody>
        </table>
    </div>`;

    return html;
}

function parseShowEventsData(data) {
    const lines = data.split('\n');
    const events = [];

    // Skip the header line and process the rest
    let startProcessing = false;

    for (const line of lines) {
        if (line.includes('Event Types (number of events):')) {
            startProcessing = true;
            continue;
        }

        if (!startProcessing || line.trim() === '') {
            continue;
        }

        // Parse each line - events can be in format "EventName (count)" or just "EventName"
        const eventMatches = line.match(/\b([A-Za-z][A-Za-z0-9_]*)\s*(\((\d+)\))?\s*/g);

        if (eventMatches) {
            for (const match of eventMatches) {
                const eventMatch = match.trim().match(/^([A-Za-z][A-Za-z0-9_]*)\s*(\((\d+)\))?/);
                if (eventMatch) {
                    const eventName = eventMatch[1];
                    const count = eventMatch[3] ? parseInt(eventMatch[3]) : 0;

                    events.push({
                        name: eventName,
                        count: count,
                        hasData: count > 0
                    });
                }
            }
        }
    }

    // Sort events: first by whether they have data (count > 0), then alphabetically
    events.sort((a, b) => {
        if (a.hasData && !b.hasData) return -1;
        if (!a.hasData && b.hasData) return 1;
        return a.name.localeCompare(b.name);
    });

    return events;
}

// Function to parse SHOW EVENTS output into HTML table (without Has Data column)
function showEventsData(events) {

    // Generate HTML table
    let html = '<div class="show-events-summary">';

    const eventsWithData = events.filter(e => e.hasData);
    const eventsWithoutData = events.filter(e => !e.hasData);

    html += `<p><strong>Total Event Types:</strong> ${events.length} (${eventsWithData.length} with data, ${eventsWithoutData.length} without data)</p>`;
    html += '</div>';

    html += '<table class="jfr-table sortable-table show-events-table">';

    // Table header (removed Has Data column)
    html += '<thead><tr>';
    html += '<th style="text-align: left" data-sort="string">Event Type <span class="sort-indicator"></span></th>';
    html += '<th style="text-align: right" data-sort="number">Count <span class="sort-indicator"></span></th>';
    html += '</tr></thead>';

    // Table body
    html += '<tbody>';
    for (const event of events) {
        const rowClass = event.hasData ? 'has-data' : 'no-data';
        const countDisplay = event.count > 0 ? event.count.toLocaleString() : '-';

        html += `<tr class="${rowClass}">`;
        html += `<td style="text-align: left" data-value="${escapeHtml(event.name)}">${escapeHtml(event.name)}</td>`;
        html += `<td style="text-align: right" data-value="${event.count}">${countDisplay}</td>`;
        html += '</tr>';
    }
    html += '</tbody></table>';

    return html;
}

function parseValueWithUnit(value) {
    if (!value || value === '-' || value === '') return NaN;

    // Remove any whitespace
    value = value.trim();

    // Handle pure numbers
    const pureNumber = parseFloat(value);
    if (!isNaN(pureNumber) && value.match(/^-?\d+([.,]\d+)?$/)) {
        return pureNumber;
    }

    // Handle time units (ns, μs, ms, s, m, h, d)
    const timeMatch = value.match(/^(\d+(?:[.,]\d+)?)\s*(ns|μs|ms|s|m|h|d)$/i);
    if (timeMatch) {
        const num = parseFloat(timeMatch[1]);
        const unit = timeMatch[2].toLowerCase();

        // Convert to consistent unit (milliseconds)
        switch (unit) {
            case 'ns': return num / 1000000;
            case 'μs': return num / 1000;
            case 'ms': return num;
            case 's': return num * 1000;
            case 'm': return num * 60000;
            case 'h': return num * 3600000;
            case 'd': return num * 86400000;
            default: return num;
        }
    }

    // Handle memory units (B, KB, MB, GB, TB, KiB, MiB, GiB, TiB)
    const memoryMatch = value.match(/^(\d+(?:[.,]\d+)?)\s*(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)$/i);
    if (memoryMatch) {
        const num = parseFloat(memoryMatch[1]);
        const unit = memoryMatch[2].toLowerCase();

        // Convert to bytes
        const multipliers = {
            'b': 1,
            'kb': 1000, 'kib': 1024,
            'mb': 1000000, 'mib': 1048576,
            'gb': 1000000000, 'gib': 1073741824,
            'tb': 1000000000000, 'tib': 1099511627776
        };

        return num * (multipliers[unit] || 1);
    }

    // Handle percentages
    const percentMatch = value.match(/^(\d+(?:[.,]\d+)?)\s*%$/);
    if (percentMatch) {
        return parseFloat(percentMatch[1].replace(",", "."));
    }

    // Handle comma-separated numbers (like 1,234,567)
    const commaNumber = value.replace(/,/g, '');
    const commaNumberParsed = parseFloat(commaNumber);
    if (!isNaN(commaNumberParsed)) {
        return commaNumberParsed;
    }

    return NaN;
}

// Enhanced parseTabularData to conditionally show selection controls
function parseTabularData(data) {
    const lines = data.split('\n');
    let headerLine = '';
    let separatorLine = '';
    let dataLines = [];
    let separatorIndex = -1;

    // Find the separator line (existing logic)
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (line.match(/^-+(\s+-+)+$/)) {
            separatorIndex = i;
            separatorLine = line;
            if (i > 0) {
                headerLine = lines[i - 1];
            }
            for (let j = i + 1; j < lines.length; j++) {
                if (lines[j].trim()) {
                    dataLines.push(lines[j]);
                }
            }
            break;
        }
    }

    if (separatorIndex === -1 || !headerLine) {
        return data; // Not a valid table format, return original
    }

    // Parse column positions (existing logic)
    const columnPositions = [];
    let currentStart = -1;
    let inColumn = false;

    for (let i = 0; i < separatorLine.length; i++) {
        const char = separatorLine[i];
        if (char === '-') {
            if (!inColumn) {
                inColumn = true;
                currentStart = i;
            }
        } else if (char === ' ') {
            if (inColumn) {
                columnPositions.push({
                    start: currentStart,
                    end: i - 1,
                    width: i - currentStart
                });
                inColumn = false;
            }
        }
    }

    if (inColumn) {
        columnPositions.push({
            start: currentStart,
            end: separatorLine.length - 1,
            width: separatorLine.length - currentStart
        });
    }

    // Parse headers
    const headers = [];
    for (const col of columnPositions) {
        const headerText = headerLine.substring(col.start, col.end + 1);
        headers.push(headerText.trim());
    }

    // Check if show graph is enabled to determine whether to show selection controls
    const showGraphEnabled = document.getElementById('showGraph') ? document.getElementById('showGraph').checked : false;

    // Generate HTML table with conditional selection controls
    let html = '';
    
    // Only add selection controls if graph is enabled
    if (showGraphEnabled) {
        html += '<div class="table-selection-controls">';
        html += '<div class="selection-buttons">';
        html += '<button onclick="toggleSelectMode()" id="selectModeBtn" class="select-mode-btn">Enable Row Selection</button>';
        html += '<button onclick="selectAllRows()" class="select-action-btn" style="display:none;">Select All</button>';
        html += '<button onclick="clearSelection()" class="select-action-btn" style="display:none;">Clear Selection</button>';
        html += '<span class="selection-count" style="display:none;">0 rows selected</span>';
        html += '<span class="selection-hint" style="display:none; margin-left: 15px; font-size: 12px; color: #666;">💡 Hold Shift and click to select a range • Graph updates automatically</span>';
        html += '</div>';
        html += '</div>'; // Close table-selection-controls div
    }

    html += '<table class="jfr-table sortable-table selectable-table">';

    // Table header with conditional select all checkbox
    html += '<thead><tr>';
    if (showGraphEnabled) {
        html += '<th class="select-column" style="display:none;"><input type="checkbox" id="selectAllCheckbox" onchange="toggleSelectAll()"></th>';
    }
    for (const header of headers) {
        const sortType = detectSortType(dataLines, columnPositions[headers.indexOf(header)]);
        html += `<th data-sort="${sortType}">${escapeHtml(header)} <span class="sort-indicator"></span></th>`;
    }
    html += '</tr></thead>';

    // Table body with conditional selectable rows
    html += '<tbody>';
    for (let rowIndex = 0; rowIndex < dataLines.length; rowIndex++) {
        const line = dataLines[rowIndex];
        const rowClickHandler = showGraphEnabled ? `onclick="toggleRowSelection(${rowIndex}, event)"` : '';
        html += `<tr data-row-index="${rowIndex}" ${rowClickHandler}>`;
        
        if (showGraphEnabled) {
            html += `<td class="select-column" style="display:none;"><input type="checkbox" onchange="handleRowCheckbox(${rowIndex}, event)" onclick="event.stopPropagation()"></td>`;
        }
        
        for (const col of columnPositions) {
            const cellText = line.substring(col.start, Math.min(col.end + 1, line.length));
            const trimmedCell = cellText.trim();
            const numericValue = parseValueWithUnit(trimmedCell);
            const dataValue = !isNaN(numericValue) ? numericValue : trimmedCell;

            html += `<td data-value="${escapeHtml(dataValue.toString())}">${escapeHtml(trimmedCell)}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody></table>';

    return html;
}

function detectSortType(dataLines, columnPosition) {
    // Sample a few values to determine the sort type
    const sampleSize = Math.min(5, dataLines.length);
    let numericCount = 0;

    for (let i = 0; i < sampleSize; i++) {
        const line = dataLines[i];
        const cellText = line.substring(columnPosition.start, Math.min(columnPosition.end + 1, line.length));
        const trimmedCell = cellText.trim();

        if (trimmedCell && trimmedCell !== '-') {
            const numericValue = parseValueWithUnit(trimmedCell);
            if (!isNaN(numericValue)) {
                numericCount++;
            }
        }
    }

    // If more than half the sampled values are numeric, treat as number
    return numericCount > sampleSize / 2 ? 'number' : 'string';
}

function makeSortable(table) {
    const headers = table.querySelectorAll('th[data-sort]');

    headers.forEach((header, index) => {
        header.addEventListener('click', () => {
            const sortType = header.getAttribute('data-sort');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));

            // Determine sort direction
            const isCurrentlyAsc = header.classList.contains('sort-asc');
            const sortAsc = !isCurrentlyAsc;

            // Clear all sort classes
            headers.forEach(h => {
                h.classList.remove('sort-asc', 'sort-desc');
                const indicator = h.querySelector('.sort-indicator');
                if (indicator) indicator.textContent = '';
            });

            // Set current sort class and indicator
            header.classList.add(sortAsc ? 'sort-asc' : 'sort-desc');
            const indicator = header.querySelector('.sort-indicator');
            if (indicator) {
                indicator.textContent = sortAsc ? '↑' : '↓';
            }

            // Sort rows
            rows.sort((a, b) => {
                const aCell = a.children[index];
                const bCell = b.children[index];

                let aValue = aCell.getAttribute('data-value') || aCell.textContent.trim();
                let bValue = bCell.getAttribute('data-value') || bCell.textContent.trim();

                if (sortType === 'number') {
                    aValue = parseFloat(aValue) || 0;
                    bValue = parseFloat(bValue) || 0;
                    return sortAsc ? aValue - bValue : bValue - aValue;
                } else {
                    aValue = aValue.toLowerCase();
                    bValue = bValue.toLowerCase();
                    if (aValue < bValue) return sortAsc ? -1 : 1;
                    if (aValue > bValue) return sortAsc ? 1 : -1;           
                    return 0;
                }
            });

            // Re-append sorted rows
            rows.forEach(row => tbody.appendChild(row));
        });
    });
}

// Enhanced executeQuery to handle show graph state changes
function executeQuery(addToHistory = false) {
    const query = queryInput.value;
    if (!query) return;

    // Hide autocompletion when executing query
    hideAutoCompleteSuggestions();

    // Skip if this exact query was just executed
    if (query === lastExecutedQuery) return;

    lastExecutedQuery = query;

    // Clear selection state when executing a new query
    selectedRows.clear();
    isSelectMode = false;
    lastSelectedRowIndex = -1;

    // Only add to history if explicitly requested (via button click)
    if (addToHistory && !previousQueries.has(query)) {
        previousQueries.add(query);
        updatePreviousQueriesDropdown();
    }

    document.getElementById('result').textContent = 'Executing query...';

    fetch(`/query?q=${encodeURIComponent(query)}`)
        .then(response => response.text())
        .then(data => {
            const resultDiv = document.getElementById('result');
            const formatOutputEnabled = document.getElementById('formatTables').checked;
            const showGraphEnabled = document.getElementById('showGraph') ? document.getElementById('showGraph').checked : false;

            // Store raw data for potential graph use
            let rawTableData = null;

            // Apply different formatting based on query type and format setting
            if (formatOutputEnabled && query.trim().toLowerCase() === 'views') {
                resultDiv.innerHTML = highlightViewsOutput(data);
            } else if (formatOutputEnabled && query.trim().toUpperCase() === 'SHOW EVENTS' && isShowEventsData(data)) {
                resultDiv.innerHTML = showEventsData(parseShowEventsData(data));
            } else if (formatOutputEnabled && query.trim().toUpperCase().startsWith('SHOW FIELDS') && isShowFieldsData(data)) {
                resultDiv.innerHTML = parseShowFieldsData(data);
            } else if (formatOutputEnabled && isTabularData(data)) {
                rawTableData = data; // Store for graph
                resultDiv.innerHTML = parseTabularData(data);
            } else {
                if (isTabularData(data)) {
                    rawTableData = data; // Store for graph even if formatting is off
                }
                resultDiv.textContent = data;
            }

            graphData = parseDataForGraph(data);
            console.log('Graph data:', graphData);

            // Handle graph display
            if (showGraphEnabled && rawTableData && isTabularData(rawTableData)) {
                if (graphContainer) {
                    graphContainer.style.display = 'block';
                    createGraph(graphData);
                }
            } else {
                if (graphContainer) {
                    graphContainer.style.display = 'none';
                }
                if (graphChart) {
                    graphChart.destroy();
                    graphChart = null;
                }
            }

            // Make tables sortable after rendering
            if (formatOutputEnabled) {
                const tables = resultDiv.querySelectorAll('.sortable-table');
                tables.forEach(table => makeSortable(table));
            }

            // Check if the error message contains a SHOW FIELDS suggestion
            checkAndExecuteShowFields(data);

            // If this was a SHOW EVENTS query, refresh our event types
            if (query.trim().toUpperCase() === 'SHOW EVENTS') {
                setTimeout(loadAvailableEventTypes, 100);
            }
        })
        .catch(error => {
            document.getElementById('result').textContent = 'Error: ' + error;
            // Hide graph on error
            if (graphContainer) {
                graphContainer.style.display = 'none';
            }
            if (graphChart) {
                graphChart.destroy();
                graphChart = null;
            }
        });
}

// Add event listener for show graph checkbox to refresh table display
document.getElementById('showGraph').addEventListener('change', function() {
    if (this.checked && graphData) {
        graphContainer.style.display = 'block';
        createGraph(graphData);
    } else {
        graphContainer.style.display = 'none';
        if (graphChart) {
            graphChart.destroy();
            graphChart = null;
        }
        
        // Clear selection state when hiding graph
        selectedRows.clear();
        isSelectMode = false;
        lastSelectedRowIndex = -1;
    }
    
    // Re-render the table with/without selection controls
    if (lastExecutedQuery) {
        const formatOutputEnabled = document.getElementById('formatTables').checked;
        const resultDiv = document.getElementById('result');
        const currentContent = resultDiv.textContent || resultDiv.innerHTML;
        
        // Only re-render if we have tabular data
        if (currentContent !== 'Executing query...' && 
            currentContent !== 'Results will appear here' && 
            formatOutputEnabled) {
            
            // Re-execute to refresh the table display
            const query = lastExecutedQuery;
            fetch(`/query?q=${encodeURIComponent(query)}`)
                .then(response => response.text())
                .then(data => {
                    if (isTabularData(data)) {
                        resultDiv.innerHTML = parseTabularData(data);
                        
                        // Make tables sortable after rendering
                        const tables = resultDiv.querySelectorAll('.sortable-table');
                        tables.forEach(table => makeSortable(table));
                    }
                })
                .catch(error => {
                    console.warn('Error refreshing table display:', error);
                });
        }
    }
});

let graphChart = null;
let graphContainer = null;
let graphCanvas = null;
let graphData = null;
let graphParseCache = new Map();
const MAX_GRAPH_CACHE_SIZE = 20;
let zeroYAxis = false;

// Add these variables near the other graph-related variables around line 1010
let isSelecting = false;
let selectionStart = null;
let selectionEnd = null;
let selectionOverlay = null;

function toggleZeroYAxis() {
    const checkbox = document.getElementById('zeroYAxisCheckbox');
    zeroYAxis = checkbox.checked;
    
    // Update the chart if it exists
    if (graphChart) {
        graphChart.options.scales.y.min = zeroYAxis ? 0 : undefined;
        graphChart.update();
    }
}

function parseDataForGraph(data) {
    // Check cache first
    const cacheKey = data.substring(0, 1000); // Use first 1000 chars as cache key
    if (graphParseCache.has(cacheKey)) {
        return graphParseCache.get(cacheKey);
    }

    const lines = data.split('\n');
    let headerLine = '';
    let separatorLine = '';
    let dataLines = [];
    let separatorIndex = -1;

    // Find the separator line (reuse existing logic)
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (line.match(/^-+(\s+-+)+$/)) {
            separatorIndex = i;
            separatorLine = line;
            if (i > 0) {
                headerLine = lines[i - 1];
            }
            for (let j = i + 1; j < lines.length; j++) {
                if (lines[j].trim()) {
                    dataLines.push(lines[j]);
                }
            }
            break;
        }
    }

    if (separatorIndex === -1 || !headerLine) {
        return null; // Not a valid table format
    }

    // Parse column positions (reuse existing logic)
    const columnPositions = [];
    let currentStart = -1;
    let inColumn = false;

    for (let i = 0; i < separatorLine.length; i++) {
        const char = separatorLine[i];
        if (char === '-') {
            if (!inColumn) {
                inColumn = true;
                currentStart = i;
            }
        } else if (char === ' ') {
            if (inColumn) {
                columnPositions.push({
                    start: currentStart,
                    end: i - 1,
                    width: i - currentStart
                });
                inColumn = false;
            }
        }
    }

    if (inColumn) {
        columnPositions.push({
            start: currentStart,
            end: separatorLine.length - 1,
            width: separatorLine.length - currentStart
        });
    }

    // Parse headers
    const headers = [];
    for (const col of columnPositions) {
        const headerText = headerLine.substring(col.start, col.end + 1);
        headers.push(headerText.trim());
    }

    // Parse data and identify time and numeric columns
    const parsedData = [];
    let timeColumnIndex = -1;
    const numericColumns = [];

    // Find time column (startTime is most common)
    for (let i = 0; i < headers.length; i++) {
        const header = headers[i].toLowerCase();
        if (header.includes('time') || header.includes('timestamp')) {
            timeColumnIndex = i;
            break;
        }
    }

    // Parse all data rows
    for (const line of dataLines) {
        const row = {};
        const rawValues = [];

        for (let i = 0; i < columnPositions.length; i++) {
            const col = columnPositions[i];
            const cellText = line.substring(col.start, Math.min(col.end + 1, line.length));
            const trimmedCell = cellText.trim();
            rawValues.push(trimmedCell);
            row[headers[i]] = trimmedCell;
        }

        row._rawValues = rawValues;
        parsedData.push(row);
    }

    // Identify numeric columns by analyzing data
    for (let i = 0; i < headers.length; i++) {
        if (i === timeColumnIndex) continue; // Skip time column

        let numericCount = 0;
        let totalCount = 0;

        for (const row of parsedData) {
            const value = row._rawValues[i];
            if (value && value !== '-') {
                totalCount++;
                const numericValue = parseValueWithUnit(value);
                if (!isNaN(numericValue) && numericValue !== 0) {
                    numericCount++;
                }
            }
        }

        // Consider column numeric if >50% of values are numeric
        if (totalCount > 0 && numericCount / totalCount > 0.5) {
            numericColumns.push({
                index: i,
                header: headers[i],
                type: detectColumnType(parsedData.map(row => row._rawValues[i]))
            });
        }
    }

    const result = {
        headers,
        data: parsedData,
        timeColumnIndex,
        numericColumns,
        columnPositions
    };

    // Add to cache with size limit
    if (graphParseCache.size >= MAX_GRAPH_CACHE_SIZE) {
        const firstKey = graphParseCache.keys().next().value;
        graphParseCache.delete(firstKey);
    }
    graphParseCache.set(cacheKey, result);

    return result;
}

function detectColumnType(values) {
    const sample = values.filter(v => v && v !== '-').slice(0, 10);

    for (const value of sample) {
        if (value.match(/^(\d+(?:[.,]\d+)?)\s*(ns|μs|ms|s|m|h|d)$/i)) {
            return 'time';
        }
        if (value.match(/^(\d+(?:[.,]\d+)?)\s*(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)$/i)) {
            return 'memory';
        }
        if (value.match(/^(\d+(?:[.,]\d+)?)\s*%$/)) {
            return 'percentage';
        }
    }
    return 'number';
}

function parseTimeValue(timeStr) {
    // Parse time strings and convert to milliseconds since epoch
    if (!timeStr || timeStr === '-') return null;

    // Handle various time formats
    // ISO format: 2023-10-15T14:30:25.123Z
    if (timeStr.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
        return new Date(timeStr).getTime();
    }

    if (timeStr.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/)) {
        // Handle local datetime format: 2023-10-15 14:30:25
        return new Date(timeStr.replace(' ', 'T') + 'Z').getTime();
    }

    if (timeStr.match(/^\d{2}:\d{2}:\d{2}/)) {
        // Handle time format: 14:30:25
        const parts = timeStr.split(':');
        const now = new Date();
        now.setHours(parseInt(parts[0], 10));
        now.setMinutes(parseInt(parts[1], 10));
        now.setSeconds(parseInt(parts[2], 10));
        return now.getTime();
    }

    // JFR timestamp format (nanoseconds since JVM start or epoch)
    // These are usually large numbers like 123456789012345
    const num = parseFloat(timeStr);
    if (!isNaN(num)) {
        // If it's a very large number (likely nanoseconds), convert appropriately
        if (num > 1e12) {
            // Likely nanoseconds since epoch or JVM start
            // Convert nanoseconds to milliseconds
            return num / 1000000;
        } else if (num > 1e9) {
            // Likely milliseconds since epoch or JVM start
            return num;
        } else if (num > 0) {
            // Smaller numbers - likely seconds since JVM start
            // Create relative timestamps from a base time
            const baseTime = Date.now() - 300000; // 5 minutes ago as base
            return baseTime + (num * 1000);
        }
    }

    // Relative time format: 12.345s, 1.2ms, etc.
    const relativeMatch = timeStr.match(/^(\d+(?:[.,]\d+)?)\s*(ns|μs|ms|s|m|h|d)$/i);
    if (relativeMatch) {
        const num = parseFloat(relativeMatch[1]);
        const unit = relativeMatch[2].toLowerCase();

        let milliseconds = 0;
        switch (unit) {
            case 'ns': milliseconds = num / 1000000; break;
            case 'μs': milliseconds = num / 1000; break;
            case 'ms': milliseconds = num; break;
            case 's': milliseconds = num * 1000; break;
            case 'm': milliseconds = num * 60000; break;
            case 'h': milliseconds = num * 3600000; break;
            case 'd': milliseconds = num * 86400000; break;
        }

        // For relative times, create timestamps relative to a base time
        const baseTime = Date.now() - 300000; // 5 minutes ago as base
        return baseTime + milliseconds;
    }

    return null;
}

function debounce(func, wait) {
    let timeout;
    return function() {
        const context = this;
        const args = arguments;
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(context, args), wait);
    };
}

function throttle(func, limit) {
    let inThrottle;
    return function() {
        const args = arguments;
        const context = this;
        if (!inThrottle) {
            func.apply(context, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// ...existing code...
// Enhance the createGraph function to add slice selection capability
function createGraph(parsedData) {
    if (!parsedData || !parsedData.numericColumns.length) {
        showGraphStatus('No numeric columns found for plotting');
        return;
    }
    
    if (parsedData.timeColumnIndex === -1) {
        showGraphStatus('No time column found for plotting');
        return;
    }
    
    // Parse and sort data by time to ensure proper ordering
    const timeData = [];
    for (const row of parsedData.data) {
        const timeValue = parseTimeValue(row._rawValues[parsedData.timeColumnIndex]);
        if (timeValue !== null) {
            const dataPoint = { time: timeValue, row: row };
            timeData.push(dataPoint);
        }
    }
    
    // Sort by time
    timeData.sort((a, b) => a.time - b.time);
    
    if (timeData.length === 0) {
        showGraphStatus('No valid time data found for plotting');
        return;
    }

    // **FIX: Better detection of time format consistency**
    const timeRange = timeData[timeData.length - 1].time - timeData[0].time;
    const firstTime = timeData[0].time;
    const lastTime = timeData[timeData.length - 1].time;
    
    // Detect if times are in the same day (likely HH:mm:ss format)
    const firstDate = new Date(firstTime);
    const lastDate = new Date(lastTime);
    const isSameDay = firstDate.toDateString() === lastDate.toDateString();
    
    // If we have relative times or same-day times, ensure proper spacing
    if ((timeRange < 1000 * 60 * 60 && timeData.length > 1) || 
        (isSameDay && timeRange < 1000 * 60 * 60 * 24)) {
        
        // For same-day data or relative times, ensure even spacing
        const startTime = isSameDay ? firstTime : (Date.now() - (timeData.length * 1000));
        const interval = timeRange / (timeData.length - 1) || 1000; // 1 second default
        
        timeData.forEach((item, index) => {
            if (isSameDay) {
                // Keep original times for same-day data
                return;
            } else {
                // Space out relative times evenly
                item.time = startTime + (index * interval);
            }
        });
    }
    
    // Prepare data for Chart.js
    const datasets = [];
    const colors = [
        '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', 
        '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384'
    ];
    
    parsedData.numericColumns.forEach((column, index) => {
        const color = colors[index % colors.length];
        const data = [];
        
        for (const item of timeData) {
            const numValue = parseValueWithUnit(item.row._rawValues[column.index]);
            
            if (!isNaN(numValue)) {
                data.push({
                    x: item.time,
                    y: numValue,
                    rowIndex: parsedData.data.indexOf(item.row) // Store original row index
                });
            }
        }
        
        if (data.length > 0) {
            datasets.push({
                label: column.header,
                data: data,
                borderColor: color,
                backgroundColor: color + '20',
                fill: false,
                tension: 0.1,
                pointRadius: data.length > 100 ? 1 : 2,
                pointHoverRadius: 4
            });
        }
    });
    
    if (datasets.length === 0) {
        showGraphStatus('No valid data points found for plotting');
        return;
    }
    
    // Create or update chart
    const ctx = graphCanvas.getContext('2d');
    
    if (graphChart) {
        graphChart.destroy();
    }
    
    // **FIX: Improved time display format detection**
    const actualTimeRange = timeData[timeData.length - 1].time - timeData[0].time;
    const actualFirstDate = new Date(timeData[0].time);
    const actualLastDate = new Date(timeData[timeData.length - 1].time);
    const isActuallySameDay = actualFirstDate.toDateString() === actualLastDate.toDateString();
    
    // Determine appropriate time display format based on actual data characteristics
    let timeDisplayFormat;
    let tooltipFormat;
    
    if (actualTimeRange < 60000) { // Less than 1 minute
        timeDisplayFormat = {
            millisecond: 'HH:mm:ss.SSS',
            second: 'HH:mm:ss',
            minute: 'HH:mm:ss',
            hour: 'HH:mm:ss',
            day: 'HH:mm:ss',
            week: 'HH:mm:ss',
            month: 'HH:mm:ss',
            quarter: 'HH:mm:ss',
            year: 'HH:mm:ss'
        };
        tooltipFormat = 'HH:mm:ss.SSS';
    } else if (actualTimeRange < 3600000 || isActuallySameDay) { // Less than 1 hour or same day
        timeDisplayFormat = {
            millisecond: 'HH:mm:ss',
            second: 'HH:mm:ss',
            minute: 'HH:mm:ss',
            hour: 'HH:mm',
            day: 'HH:mm',
            week: 'HH:mm',
            month: 'HH:mm',
            quarter: 'HH:mm',
            year: 'HH:mm'
        };
        tooltipFormat = 'HH:mm:ss';
    } else if (actualTimeRange < 86400000) { // Less than 1 day
        timeDisplayFormat = {
            millisecond: 'HH:mm:ss',
            second: 'HH:mm:ss',
            minute: 'HH:mm',
            hour: 'HH:mm',
            day: 'MM/dd HH:mm',
            week: 'MM/dd HH:mm',
            month: 'MM/dd HH:mm',
            quarter: 'MM/dd HH:mm',
            year: 'MM/dd HH:mm'
        };
        tooltipFormat = 'MM/dd/yyyy HH:mm:ss';
    } else {
        timeDisplayFormat = {
            millisecond: 'MM/dd HH:mm:ss',
            second: 'MM/dd HH:mm:ss',
            minute: 'MM/dd HH:mm',
            hour: 'MM/dd HH:mm',
            day: 'MM/dd',
            week: 'MM/dd',
            month: 'MMM yyyy',
            quarter: 'MMM yyyy',
            year: 'yyyy'
        };
        tooltipFormat = 'MM/dd/yyyy HH:mm:ss';
    }

    // Throttled update function for pan/zoom
    const throttlePanZoomUpdate = throttle(function({chart}) {
        updateTableForCurrentViewport(chart, graphData, timeData);
    }, 100); // 100ms throttle limit
    
    graphChart = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        displayFormats: timeDisplayFormat,
                        tooltipFormat: tooltipFormat
                    },
                    title: {
                        display: true,
                        text: parsedData.headers[parsedData.timeColumnIndex]
                    },
                    ticks: {
                        source: 'auto',
                        autoSkip: true,
                        maxTicksLimit: 10,
                        callback: function(value, index, ticks) {
                            // **FIX: Consistent formatting for tick labels**
                            const date = new Date(value);
                            
                            if (actualTimeRange < 60000) {
                                // Less than 1 minute - show seconds
                                return date.toLocaleTimeString('en-US', {
                                    hour12: false,
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    second: '2-digit'
                                });
                            } else if (actualTimeRange < 3600000 || isActuallySameDay) {
                                // Less than 1 hour or same day - show minutes
                                return date.toLocaleTimeString('en-US', {
                                    hour12: false,
                                    hour: '2-digit',
                                    minute: '2-digit'
                                });
                            } else if (actualTimeRange < 86400000) {
                                // Less than 1 day - show hours and minutes
                                return date.toLocaleTimeString('en-US', {
                                    hour12: false,
                                    hour: '2-digit',
                                    minute: '2-digit'
                                });
                            } else {
                                // More than 1 day - show date and time
                                return date.toLocaleDateString('en-US', {
                                    month: 'short',
                                    day: 'numeric',
                                    hour: '2-digit',
                                    minute: '2-digit'
                                });
                            }
                        }
                    }
                },
                y: {
                    min: zeroYAxis ? 0 : undefined,
                    title: {
                        display: true,
                        text: 'Value'
                    }
                }
            },
            plugins: {
                zoom: {
                    pan: {
                        enabled: true,
                        mode: 'x',
                        modifierKey: 'shift',
                        onPan: throttlePanZoomUpdate
                    },
                    zoom: {
                        wheel: {
                            enabled: true
                        },
                        pinch: {
                            enabled: true
                        },
                        mode: 'x',
                        onZoom: throttlePanZoomUpdate
                    }
                },
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'nearest',
                    intersect: false,
                    callbacks: {
                        title: function(context) {
                            const date = new Date(context[0].parsed.x);
                            
                            // **FIX: Use same formatting logic as ticks**
                            if (actualTimeRange < 60000) {
                                return date.toLocaleTimeString('en-US', {
                                    hour12: false,
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    second: '2-digit',
                                    fractionalSecondDigits: 3
                                });
                            } else if (actualTimeRange < 3600000 || isActuallySameDay) {
                                return date.toLocaleTimeString('en-US', {
                                    hour12: false,
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    second: '2-digit'
                                });
                            } else if (actualTimeRange < 86400000) {
                                return date.toLocaleTimeString('en-US', {
                                    hour12: false,
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    second: '2-digit'
                                });
                            } else {
                                return date.toLocaleString('en-US', {
                                    year: 'numeric',
                                    month: 'short',
                                    day: 'numeric',
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    second: '2-digit',
                                    hour12: false
                                });
                            }
                        },
                        label: function(context) {
                            return `${context.dataset.label}: ${context.parsed.y}`;
                        }
                    }
                }
            }
        }
    });
        
    updateColumnSelector(parsedData.numericColumns);
    showGraphStatus('');

    if (graphChart) {
        // Set responsive to false and use resize() manually for better control
        graphChart.options.responsive = false;
        graphChart.resize();
    }
    
    console.log(`Created graph with ${datasets.length} datasets and ${timeData.length} time points`);
    console.log('Time range:', new Date(timeData[0].time).toLocaleString(), 'to', new Date(timeData[timeData.length - 1].time).toLocaleString());
    console.log('Same day data:', isActuallySameDay, 'Time range (ms):', actualTimeRange);
}

// Update the resetGraphZoom function to also clear selection
function resetGraphZoom() {
    if (graphChart) {
        graphChart.resetZoom();
        // Also reset table filter when resetting zoom
        resetTableFilter();
    }
}

function updateTableForCurrentViewport(chart, parsedData, timeData) {
    // Get the current x-axis range from the chart
    const xScale = chart.scales.x;
    const minTime = xScale.min;
    const maxTime = xScale.max;
    
    console.log('Viewport time range:', new Date(minTime).toLocaleString(), 'to', new Date(maxTime).toLocaleString());
    
    // Find all table rows in the result div
    const resultDiv = document.getElementById('result');
    const tables = resultDiv.querySelectorAll('.jfr-table');
    
    if (tables.length === 0) {
        console.log('No tables found to filter');
        return;
    }
    
    // Process each table (there should typically be only one)
    tables.forEach(table => {
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        
        const rows = Array.from(tbody.querySelectorAll('tr'));
        let visibleCount = 0;
        
        // Show/hide rows based on their time values
        rows.forEach((row, index) => {
            if (index >= timeData.length) {
                // More table rows than data points, hide extra rows
                row.style.display = 'none';
                return;
            }
            
            const timeValue = timeData[index].time;
            
            if (timeValue >= minTime && timeValue <= maxTime) {
                row.style.display = '';
                visibleCount++;
            } else {
                row.style.display = 'none';
            }
        });
        
        console.log(`Filtered table: showing ${visibleCount} of ${rows.length} rows`);
        
        // Add or update viewport info
        updateViewportInfo(table, visibleCount, rows.length, minTime, maxTime);
    });
}

function updateViewportInfo(table, visibleCount, totalCount, minTime, maxTime) {
    // Remove existing viewport info if present
    const existingInfo = table.parentNode.querySelector('.viewport-info');
    if (existingInfo) {
        existingInfo.remove();
    }
    
    // Create new viewport info
    const viewportInfo = document.createElement('div');
    viewportInfo.className = 'viewport-info';
    viewportInfo.innerHTML = `<div class="viewport-summary">
            <strong>Current View:</strong> ${visibleCount} of ${totalCount} rows <span class="viewport-time-range">(${new Date(minTime).toLocaleTimeString()} - ${new Date(maxTime).toLocaleTimeString()})</span>
            <button onclick="resetTableFilter()" class="reset-filter-btn">Show All Rows</button>
        </div>`;
    
    // Insert before the table
    table.parentNode.insertBefore(viewportInfo, table);
}

function resetTableFilter() {
    // Show all table rows
    const resultDiv = document.getElementById('result');
    const tables = resultDiv.querySelectorAll('.jfr-table');
    
    tables.forEach(table => {
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        
        const rows = tbody.querySelectorAll('tr');
        rows.forEach(row => {
            row.style.display = '';
        });
        
        // Remove viewport info
        const viewportInfo = table.parentNode.querySelector('.viewport-info');
        if (viewportInfo) {
            viewportInfo.remove();
        }
    });
    
    // Reset chart zoom
    if (graphChart) {
        graphChart.resetZoom();
    }
    
    console.log('Reset table filter and chart zoom');
}

function updateColumnSelector(numericColumns) {
    const selector = document.querySelector('.column-selector');
    selector.innerHTML = '';

    numericColumns.forEach((column, index) => {
        const checkbox = document.createElement('div');
        checkbox.className = 'column-checkbox';
        checkbox.innerHTML = `
            <input type="checkbox" id="col_${index}" checked onchange="toggleGraphColumn(${index})">
            <label for="col_${index}">${column.header}</label>
        `;
        selector.appendChild(checkbox);
    });
}

function toggleGraphColumn(columnIndex) {
    if (!graphChart) return;

    const dataset = graphChart.data.datasets[columnIndex];
    if (dataset) {
        const meta = graphChart.getDatasetMeta(columnIndex);
        meta.hidden = !meta.hidden;
        graphChart.update();
    }
}

function showGraphStatus(message) {
    const statusDiv = document.querySelector('.graph-status');
    if (message) {
        statusDiv.textContent = message;
        statusDiv.style.display = 'block';
        graphCanvas.style.display = 'none';
    } else {
        statusDiv.style.display = 'none';
        graphCanvas.style.display = 'block';
    }
}

// Enhanced function to load available event types dynamically
async function loadAvailableEventTypes() {
    try {
        const response = await fetch('/query?q=' + encodeURIComponent('SHOW EVENTS'));
        const data = await response.text();
        const parsed = parseShowEventsData(data);

        const eventTypes = [];
        const eventCounts = {};
        parsed.forEach(element => {
            if (element.count == 0) {
                return;
            }
            eventCounts[element.name] = element.count;
            eventTypes.push(element.name);
        });

        // Update the vocabulary with actual available events and their counts
        if (parsed.length > 0) {
            queryVocabulary.availableEventTypes = eventTypes;
            queryVocabulary.eventCounts = eventCounts;
            console.log(`Loaded ${eventTypes.length} event types with data:`, eventTypes);

            // Filter sample queries after loading events
            filterSampleQueriesByAvailableEvents();
        }

    } catch (error) {
        console.warn('Could not load event types, using fallback list:', error);
        // Keep the fallback list that's already in availableEventTypes
    }
}

// Enhanced filterSampleQueriesByAvailableEvents to also update dropdown
function filterSampleQueriesByAvailableEvents() {
    const originalCount = queryVocabulary.sampleQueries.length;

    // Filter the sample queries to only include those with available events
    queryVocabulary.sampleQueries = queryVocabulary.sampleQueries.filter(query => {
        const queryText = typeof query === 'string' ? query : query.text;
        return isQueryValidForDataset(queryText);
    });

    const filteredCount = queryVocabulary.sampleQueries.length;
    console.log(`Filtered sample queries: ${originalCount} -> ${filteredCount} (removed ${originalCount - filteredCount} queries with unavailable events)`);

    // Also filter the previousQueries Set to remove invalid queries
    const previousQueriesList = Array.from(previousQueries);
    previousQueries.clear();

    previousQueriesList.forEach(query => {
        const queryText = typeof query === 'string' ? query : query.text;
        if (isQueryValidForDataset(queryText)) {
            previousQueries.add(query);
        }
    });

    // Update the dropdown with filtered queries
    updatePreviousQueriesDropdown();
}

// New function to check if a query references only available events
function isQueryValidForDataset(queryText) {
    // Always allow non-event queries (SHOW commands, grammar, views, etc.)
    if (!queryText.includes('FROM') ||
        queryText.toLowerCase().includes('show ') ||
        queryText === 'grammar' ||
        queryText === 'views') {
        return true;
    }

    // Extract event names from FROM clauses
    const fromMatches = queryText.match(/FROM\s+([A-Za-z][A-Za-z0-9_.]*)/gi);
    if (!fromMatches) {
        return true; // No FROM clause found, allow it
    }

    // Check if all referenced events are in our available event types
    for (const fromMatch of fromMatches) {
        const eventName = fromMatch.replace(/FROM\s+/i, '').trim();

        // Check if this event is in our available events list
        if (!queryVocabulary.availableEventTypes.includes(eventName)) {
            return false; // Event not available in dataset
        }
    }

    return true; // All events are available
}

// Enhanced updatePreviousQueriesDropdown to filter by available events
function updatePreviousQueriesDropdown() {
    const select = document.getElementById('previousQueries');

    // Clear existing options except the first one
    while (select.options.length > 1) {
        select.remove(1);
    }

    const specialOptions = ["grammar", "views"];
    specialOptions.forEach(query => {
        if (!previousQueries.has(query)) {
            const option = document.createElement('option');
            option.value = query;
            option.text = query;
            select.add(option);
        }
    });

    // Filter and sort queries from previousQueries Set
    Array.from(previousQueries).filter(query => {
        // Handle both string and object queries
        const queryText = typeof query === 'string' ? query : query.text;
        // Apply the same filtering logic as suggestions
        return isQueryValidForDataset(queryText);
    }).sort((a, b) => {
        // Handle both string and object queries for sorting
        const aText = typeof a === 'string' ? a : a.text;
        const bText = typeof b === 'string' ? b : b.text;
        return aText.localeCompare(bText);
    }).forEach(query => {
        const option = document.createElement('option');

        // Handle both string and object queries
        const queryText = typeof query === 'string' ? query : query.text;
        const queryDescription = typeof query === 'string' ? '' : query.description;

        option.value = queryText;

        // Create display text with description if available
        let displayText = queryText.length > 50 ? queryText.substring(0, 50) + '...' : queryText;
        if (queryDescription) {
            displayText += ` (${queryDescription})`;
        }

        option.text = displayText;
        select.add(option);
    });

    // Also add sample queries that are valid for the dataset
    queryVocabulary.sampleQueries.forEach(query => {
        const queryText = typeof query === 'string' ? query : query.text;
        const queryDescription = typeof query === 'string' ? '' : query.description;

        // Filter out queries that reference events not in the dataset
        if (!isQueryValidForDataset(queryText)) {
            return; // Skip this query
        }

        // Skip if already in previousQueries
        if (previousQueries.has(queryText) || previousQueries.has(query)) {
            return;
        }

        const option = document.createElement('option');
        option.value = queryText;

        // Create display text with description if available
        let displayText = queryText.length > 50 ? queryText.substring(0, 50) + '...' : queryText;
        if (queryDescription) {
            displayText += ` (${queryDescription})`;
        }

        option.text = displayText;
        select.add(option);
    });
}

// Enhanced function to add sample query suggestions - filter by available events
function addSampleQuerySuggestions(suggestions, currentWord) {
    queryVocabulary.sampleQueries.forEach(query => {
        // Handle both string and object formats
        const queryText = typeof query === 'string' ? query : query.text;
        const queryType = typeof query === 'string' ? 'sample' : (query.type || 'sample');
        const queryDescription = typeof query === 'string' ? '' : (query.description || '');

        // Filter out queries that reference events not in the dataset
        if (!isQueryValidForDataset(queryText)) {
            return; // Skip this query
        }

        if (currentWord.length === 0 ||
            queryText.toLowerCase().includes(currentWord.toLowerCase())) {

            let description = queryDescription;
            if (!description && typeof query === 'string') {
                // Generate description for string queries
                if (queryText.includes('SHOW')) {
                    description = 'Show information';
                } else if (queryText.includes('CAPTION')) {
                    description = 'Formatted report';
                } else if (queryText.includes('SELECT') && queryText.includes('GROUP BY')) {
                    description = 'Aggregation query';
                } else if (queryText.includes('SELECT')) {
                    description = 'Basic query';
                } else if (queryText === 'grammar' || queryText === 'views') {
                    description = 'Help command';
                }
            }

            suggestions.push({
                text: queryText,
                type: queryType,
                description: description
            });
        }
    });
}

// Enhanced function to parse view.ini format and extract queries with descriptions
function parseViewQueries(viewsData) {
    const viewQueries = [];
    const lines = viewsData.split('\n');
    let currentSection = null;
    let currentLabel = null;
    let inQuery = false;
    let queryLines = [];

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();

        // Skip comments and empty lines
        if (line.startsWith(';') || line === '') {
            continue;
        }

        // Section header like [environment.active-recordings]
        const sectionMatch = line.match(/^\[([^\]]+)\]$/);
        if (sectionMatch) {
            // Save previous query if exists
            if (currentSection && currentLabel && queryLines.length > 0) {
                const fullQuery = queryLines.join(' ').trim();
                if (fullQuery) {
                    const cleanQuery = extractCleanQuery(fullQuery);
                    viewQueries.push({
                        id: currentSection,
                        label: currentLabel,
                        fullQuery: fullQuery,
                        cleanQuery: cleanQuery,
                        category: currentSection.split('.')[0] // environment, application, jvm
                    });
                }
            }

            currentSection = sectionMatch[1];
            currentLabel = null;
            queryLines = [];
            inQuery = false;
            continue;
        }

        // Label line
        const labelMatch = line.match(/^label\s*=\s*"([^"]+)"$/);
        if (labelMatch) {
            currentLabel = labelMatch[1];
            continue;
        }

        // Table or form query start
        const queryStartMatch = line.match(/^(table|form)\s*=\s*"(.*)$/);
        if (queryStartMatch) {
            inQuery = true;
            queryLines = [queryStartMatch[2]];
            continue;
        }

        // Continue multi-line query
        if (inQuery) {
            if (line.endsWith('"')) {
                // End of query
                queryLines.push(line.slice(0, -1));
                inQuery = false;
            } else {
                queryLines.push(line);
            }
        }
    }

    // Handle last query if file doesn't end with proper closure
    if (currentSection && currentLabel && queryLines.length > 0) {
        const fullQuery = queryLines.join(' ').trim();
        if (fullQuery) {
            const cleanQuery = extractCleanQuery(fullQuery);
            viewQueries.push({
                id: currentSection,
                label: currentLabel,
                fullQuery: fullQuery,
                cleanQuery: cleanQuery,
                category: currentSection.split('.')[0]
            });
        }
    }

    return viewQueries;
}

// Function to extract clean query without COLUMN/FORMAT prefixes
function extractCleanQuery(fullQuery) {
    // Remove COLUMN and FORMAT directives to get the core SQL
    let cleanQuery = fullQuery;

    // Remove COLUMN directive - handle multiline case
    cleanQuery = cleanQuery.replace(/^COLUMN\s+[^S]*?(SELECT)/i, '$1');

    // Remove FORMAT directive - handle multiline case
    cleanQuery = cleanQuery.replace(/FORMAT\s+[^S]*?(SELECT)/i, '$1');

    // Clean up extra whitespace
    cleanQuery = cleanQuery.replace(/\s+/g, ' ').trim();

    return cleanQuery;
}

// Function to create CAPTION version of a query
function createCaptionQuery(viewQuery) {
    // Extract column names from COLUMN directive if present
    const columnMatch = viewQuery.fullQuery.match(/COLUMN\s+(.*?)\s+(?:FORMAT|SELECT)/is);
    if (columnMatch) {
        const columnDef = columnMatch[1].trim();
        // Create CAPTION version
        return `CAPTION ${columnDef} ${viewQuery.cleanQuery}`;
    }
    return null;
}

// Function to load view queries dynamically
async function loadViewQueries() {
    try {
        const response = await fetch('/query?q=' + encodeURIComponent('views'));
        const data = await response.text();

        const viewQueries = parseViewQueries(data);

        if (viewQueries.length > 0) {
            console.log(`Loaded ${viewQueries.length} view queries`);

            // Add view queries to sample queries in multiple formats
            viewQueries.forEach(view => {
                // Add the clean query (without COLUMN/FORMAT)
                queryVocabulary.sampleQueries.push({
                    text: view.cleanQuery,
                    type: 'view',
                    description: view.label,
                    category: view.category,
                    id: view.id
                });

                // Add the full query (with COLUMN/FORMAT if present)
                if (view.fullQuery !== view.cleanQuery) {
                    queryVocabulary.sampleQueries.push({
                        text: view.fullQuery,
                        type: 'view-formatted',
                        description: `${view.label} (formatted)`,
                        category: view.category,
                        id: view.id
                    });
                }

                // Add CAPTION version if COLUMN directive exists
                const captionQuery = createCaptionQuery(view);
                if (captionQuery) {
                    queryVocabulary.sampleQueries.push({
                        text: captionQuery,
                        type: 'view-caption',
                        description: `${view.label} (caption)`,
                        category: view.category,
                        id: view.id
                    });
                }
            });

            console.log(`Added ${queryVocabulary.sampleQueries.filter(s => s.type?.startsWith('view')).length} view-based sample queries`);
        }

    } catch (error) {
        console.warn('Could not load view queries:', error);
    }
}

// Enhanced function to check for and execute SHOW FIELDS suggestions
async function checkAndExecuteShowFields(errorMessage) {
    // Look for patterns like "SHOW FIELDS SomeEventName" in the error message
    const showFieldsMatch = errorMessage.match(/SHOW FIELDS\s+([A-Za-z][A-Za-z0-9_.]*)/);

    if (showFieldsMatch) {
        const eventName = showFieldsMatch[1];
        const showFieldsQuery = `SHOW FIELDS ${eventName}`;

        // Check cache first
        if (showFieldsCache.has(showFieldsQuery)) {
            console.log(`Using cached fields for: ${eventName}`);
            displayShowFieldsResult(showFieldsQuery, showFieldsCache.get(showFieldsQuery));
            return;
        }

        try {
            console.log(`Auto-executing: ${showFieldsQuery}`);
            const response = await fetch(`/query?q=${encodeURIComponent(showFieldsQuery)}`);
            const fieldsData = await response.text();

            // Use the enhanced cache function
            addToShowFieldsCache(showFieldsQuery, fieldsData);
            console.log(`Cached fields for: ${eventName}`);

            // Display the fields information
            displayShowFieldsResult(showFieldsQuery, fieldsData);

        } catch (error) {
            console.warn('Failed to auto-execute SHOW FIELDS:', error);
            const resultDiv = document.getElementById('result');
            const currentContent = resultDiv.textContent;
            resultDiv.textContent = currentContent + `\n\nFailed to auto-execute: ${showFieldsQuery}`;
        }
    }
}

// Function to display SHOW FIELDS results below the error message
function displayShowFieldsResult(query, fieldsData) {
    const resultDiv = document.getElementById('result');
    const currentContent = resultDiv.textContent;

    // Create a separator and add the fields information
    const separator = '\n\n' + '='.repeat(50) + '\n';
    const fieldsSection = `Auto-executed: ${query}\n${separator}${fieldsData}`;

    resultDiv.textContent = currentContent + '\n\n' + fieldsSection;

    // Scroll to show the new content
    resultDiv.scrollTop = resultDiv.scrollHeight;
}

// Enhanced cache management - add function to get cache statistics
function getShowFieldsCacheStats() {
    console.log(`SHOW FIELDS cache contains ${showFieldsCache.size} entries:`);
    for (const [query, data] of showFieldsCache.entries()) {
        console.log(`- ${query}: ${data.length} characters`);
    }
}

// Enhanced cache clear function with statistics
function clearShowFieldsCache() {
    const size = showFieldsCache.size;
    showFieldsCache.clear();
    console.log(`SHOW FIELDS cache cleared (${size} entries removed)`);
}

// Optional: Add cache size limit to prevent memory issues
const MAX_CACHE_SIZE = 50; // Maximum number of cached SHOW FIELDS results

function addToShowFieldsCache(query, data) {
    // If cache is at limit, remove oldest entry (simple LRU)
    if (showFieldsCache.size >= MAX_CACHE_SIZE) {
        const firstKey = showFieldsCache.keys().next().value;
        showFieldsCache.delete(firstKey);
        console.log(`Cache limit reached, removed: ${firstKey}`);
    }

    showFieldsCache.set(query, data);
}

// Initialize sample queries - only add strings to previousQueries Set
queryVocabulary.sampleQueries.forEach(query => {
    // Only add string queries to the dropdown, skip objects
    if (typeof query === 'string') {
        previousQueries.add(query);
    } else {
        // For object queries, add just the text part
        previousQueries.add(query.text);
    }
});

// Enhanced function to add sample query suggestions
function addSampleQuerySuggestions(suggestions, currentWord) {
    queryVocabulary.sampleQueries.forEach(query => {
        // Handle both string and object formats
        const queryText = typeof query === 'string' ? query : query.text;
        const queryType = typeof query === 'string' ? 'sample' : (query.type || 'sample');
        const queryDescription = typeof query === 'string' ? '' : (query.description || '');

        if (currentWord.length === 0 ||
            queryText.toLowerCase().includes(currentWord.toLowerCase())) {

            let description = queryDescription;
            if (!description && typeof query === 'string') {
                // Generate description for string queries
                if (queryText.includes('SHOW')) {
                    description = 'Show information';
                } else if (queryText.includes('CAPTION')) {
                    description = 'Formatted report';
                } else if (queryText.includes('SELECT') && queryText.includes('GROUP BY')) {
                    description = 'Aggregation query';
                } else if (queryText.includes('SELECT')) {
                    description = 'Basic query';
                } else if (queryText === 'grammar' || queryText === 'views') {
                    description = 'Help command';
                }
            }

            suggestions.push({
                text: queryText,
                type: queryType,
                description: description
            });
        }
    });
}

// Enhanced auto-completion function with better sample query logic
function getAutoCompleteSuggestions(text, cursorPos) {
    const beforeCursor = text.substring(0, cursorPos);
    const wordMatch = beforeCursor.match(/\b\w*$/);
    const currentWord = wordMatch ? wordMatch[0].toLowerCase() : '';

    const suggestions = [];
    const context = getQueryContext(beforeCursor);

    // Add sample queries when at the start or for short inputs
    if (beforeCursor.trim().length < 10 || context === 'start') {
        addSampleQuerySuggestions(suggestions, currentWord);
    }

    // Add context-specific suggestions
    switch (context) {
        case 'start':
        case 'after_keyword':
            addSuggestions(suggestions, queryVocabulary.keywords, currentWord, 'keyword');
            break;
        case 'after_select':
            addSuggestions(suggestions, queryVocabulary.functions, currentWord, 'function');
            addSuggestions(suggestions, queryVocabulary.commonFields, currentWord, 'field');
            break;
        case 'after_from':
            addEventSuggestions(suggestions, queryVocabulary.availableEventTypes, currentWord);
            break;
        case 'after_format':
            addSuggestions(suggestions, queryVocabulary.formatProperties, currentWord, 'property');
            break;
        default:
            addSuggestions(suggestions, queryVocabulary.keywords, currentWord, 'keyword');
            addSuggestions(suggestions, queryVocabulary.functions, currentWord, 'function');
            addSuggestions(suggestions, queryVocabulary.commonFields, currentWord, 'field');
            addEventSuggestions(suggestions, queryVocabulary.availableEventTypes, currentWord);
    }

    // Sort suggestions - DON'T prioritize view queries, keep them mixed in
    suggestions.sort((a, b) => {
        const trimmedInput = beforeCursor.trim().toLowerCase();

        // Prioritize exact matches from the beginning
        const aStartsWithInput = a.text.toLowerCase().startsWith(trimmedInput);
        const bStartsWithInput = b.text.toLowerCase().startsWith(trimmedInput);

        if (aStartsWithInput && !bStartsWithInput) return -1;
        if (!aStartsWithInput && bStartsWithInput) return 1;

        // Otherwise keep original order (mixed)
        return 0;
    });

    return suggestions.slice(0, 20);
}

// New function to add event suggestions with counts
function addEventSuggestions(suggestions, eventTypes, currentWord) {
    eventTypes.forEach(eventType => {
        if (eventType.toLowerCase().includes(currentWord.toLowerCase()) ||
            eventType.toLowerCase().startsWith(currentWord.toLowerCase())) {
            const count = queryVocabulary.eventCounts && queryVocabulary.eventCounts[eventType]
                ? queryVocabulary.eventCounts[eventType]
                : '';

            suggestions.push({
                text: eventType,
                type: 'event',
                count: count,
                description: count ? `${count} events` : ''
            });
        }
    });
}

// Enhanced suggestion display to show counts and descriptions
function showAutoCompleteSuggestions(suggestions) {
    if (suggestions.length === 0) {
        hideAutoCompleteSuggestions();
        return;
    }

    currentSuggestions = suggestions;
    selectedSuggestionIndex = -1; // Don't preselect any suggestion

    const html = suggestions.map((suggestion, index) => {
        let displayText = suggestion.text;
        let metaInfo = '';

        // Format based on suggestion type
        if (suggestion.type === 'event' && suggestion.count) {
            metaInfo = `<span class="suggestion-count">${suggestion.count} events</span>`;
        } else if (suggestion.description) {
            metaInfo = `<span class="suggestion-description">${suggestion.description}</span>`;
        }

        // Truncate very long queries for display
        if ((suggestion.type?.startsWith('view') || suggestion.type === 'sample') && displayText.length > 80) {
            displayText = displayText.substring(0, 80) + '...';
        }

        return `<div class="autocomplete-suggestion" data-index="${index}">
            <div class="suggestion-main">
                <div class="suggestion-text">${displayText}</div>
                <div class="suggestion-meta">
                    <span class="autocomplete-suggestion-type">${suggestion.type}</span>
                    ${metaInfo}
                </div>
            </div>
        </div>`;
    }).join('');

    autocompleteSuggestions.innerHTML = html;
    autocompleteSuggestions.style.display = 'block';

    // Add click handlers
    autocompleteSuggestions.querySelectorAll('.autocomplete-suggestion').forEach(elem => {
        elem.addEventListener('click', () => {
            applySuggestion(suggestions[parseInt(elem.dataset.index)]);
        });
    });
}

// Enhanced applySuggestion to handle sample queries better
function applySuggestion(suggestion) {
    const cursorPos = queryInput.selectionStart;
    const text = queryInput.value;
    const beforeCursor = text.substring(0, cursorPos);
    const afterCursor = text.substring(cursorPos);

    let newText, newPos;

    // For sample queries, replace the entire query if we're at the start
    if (suggestion.type === 'sample' && beforeCursor.trim().length < 10) {
        newText = suggestion.text;
        newPos = suggestion.text.length;
    } else {
        // For other suggestions, find the current partial word
        const wordMatch = beforeCursor.match(/\b\w*$/);
        const wordStart = wordMatch ? cursorPos - wordMatch[0].length : cursorPos;

        // Replace the partial word with the suggestion
        newText = text.substring(0, wordStart) + suggestion.text + afterCursor;
        newPos = wordStart + suggestion.text.length;
    }

    queryInput.value = newText;
    queryInput.setSelectionRange(newPos, newPos);

    hideAutoCompleteSuggestions();
    updateHighlighting();
    queryInput.focus();
}

function getQueryContext(beforeCursor) {
    const trimmed = beforeCursor.trim().toLowerCase();

    if (trimmed === '' || /^\s*$/.test(trimmed)) return 'start';
    if (/\bselect\s*$/i.test(trimmed)) return 'after_select';
    if (/\bfrom\s*$/i.test(trimmed)) return 'after_from';
    if (/\bformat\s*$/i.test(trimmed)) return 'after_format';
    if (/\bwhere\s*$/i.test(trimmed)) return 'after_where';
    if (/\bgroup\s+by\s*$/i.test(trimmed)) return 'after_group_by';
    if (/\border\s+by\s*$/i.test(trimmed)) return 'after_order_by';

    return 'general';
}

function addSuggestions(suggestions, items, currentWord, type) {
    items.forEach(item => {
        if (item.toLowerCase().startsWith(currentWord)) {
            suggestions.push({ text: item, type: type });
        }
    });
}

function hideAutoCompleteSuggestions() {
    autocompleteSuggestions.style.display = 'none';
    currentSuggestions = [];
    selectedSuggestionIndex = -1;
}

// Update navigateSuggestions to handle the case where no suggestion is initially selected
function navigateSuggestions(direction) {
    if (currentSuggestions.length === 0) return;

    // Remove previous selection
    if (selectedSuggestionIndex >= 0) {
        const prevSelected = autocompleteSuggestions.children[selectedSuggestionIndex];
        if (prevSelected) {
            prevSelected.classList.remove('selected');
        }
    }

    // Update selection
    if (selectedSuggestionIndex === -1) {
        // If no selection, start from first (down) or last (up)
        selectedSuggestionIndex = direction > 0 ? 0 : currentSuggestions.length - 1;
    } else {
        selectedSuggestionIndex += direction;
        if (selectedSuggestionIndex < 0) {
            selectedSuggestionIndex = currentSuggestions.length - 1;
        } else if (selectedSuggestionIndex >= currentSuggestions.length) {
            selectedSuggestionIndex = 0;
        }
    }

    // Add new selection
    const newSelected = autocompleteSuggestions.children[selectedSuggestionIndex];
    if (newSelected) {
        newSelected.classList.add('selected');
        newSelected.scrollIntoView({ block: 'nearest' });
    }
}

// Update the format output checkbox handler
document.getElementById('formatTables').addEventListener('change', function() {
    // If there's a current query result, re-process it
    const resultDiv = document.getElementById('result');
    const currentContent = resultDiv.textContent || resultDiv.innerHTML;

    // Only re-execute if we have a last executed query and it's not empty
    if (lastExecutedQuery &&
        currentContent !== 'Executing query...' &&
        currentContent !== 'Results will appear here' &&
        currentContent !== 'Re-formatting...') {

        // Re-execute the last query to apply/remove output formatting
        const query = lastExecutedQuery;
        document.getElementById('result').textContent = 'Re-formatting...';

        fetch(`/query?q=${encodeURIComponent(query)}`)
            .then(response => response.text())
            .then(data => {
                const formatOutputEnabled = this.checked;

                if (formatOutputEnabled && query.trim().toLowerCase() === 'views') {
                    resultDiv.innerHTML = highlightViewsOutput(data);
                } else if (formatOutputEnabled && query.trim().toUpperCase() === 'SHOW EVENTS' && isShowEventsData(data)) {
                    resultDiv.innerHTML = parseShowEventsData(data);
                } else if (formatOutputEnabled && query.trim().toUpperCase().startsWith('SHOW FIELDS') && isShowFieldsData(data)) {
                    resultDiv.innerHTML = parseShowFieldsData(data);
                } else if (formatOutputEnabled && isTabularData(data)) {
                    resultDiv.innerHTML = parseTabularData(data);
                } else {
                    resultDiv.textContent = data;
                }
            })
            .catch(error => {
                resultDiv.textContent = 'Error: ' + error;
            });
    }
});

// Function to highlight the views output with syntax highlighting
function highlightViewsOutput(viewsData) {
    const lines = viewsData.split('\n');
    const highlightedLines = [];
    let inQuery = false;
    let queryLines = [];

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        // Check if this is a section header
        if (line.match(/^\[([^\]]+)\]$/)) {
            // If we were in a query, highlight it first
            if (inQuery && queryLines.length > 0) {
                const queryText = queryLines.join('\n');
                const highlighted = highlightSyntax(queryText);
                highlightedLines.push(highlighted);
                queryLines = [];
                inQuery = false;
            }

            // Add the section header with special styling
            highlightedLines.push(`<span class="view-section">${escapeHtml(line)}</span>`);
            continue;
        }

        // Check if this is a label line
        if (line.match(/^label\s*=\s*"([^"]+)"$/)) {
            highlightedLines.push(`<span class="view-label">${escapeHtml(line)}</span>`);
            continue;
        }

        // Check if this starts a query
        if (line.match(/^(table|form)\s*=\s*"/)) {
            inQuery = true;
            queryLines = [line];
            continue;
        }

        // If we're in a query, collect lines
        if (inQuery) {
            queryLines.push(line);

            // Check if this ends the query
            if (line.trim().endsWith('"')) {
                // Highlight the complete query
                const queryText = queryLines.join('\n');
                const highlighted = highlightViewsQuery(queryText);
                highlightedLines.push(highlighted);
                queryLines = [];
                inQuery = false;
            }
            continue;
        }

        // For other lines (comments, etc.), just escape and add
        if (line.trim().startsWith(';')) {
            highlightedLines.push(`<span class="view-comment">${escapeHtml(line)}</span>`);
        } else if (line.trim() === '') {
            highlightedLines.push('');
        } else {
            highlightedLines.push(escapeHtml(line));
        }
    }

    // Handle any remaining query at the end
    if (inQuery && queryLines.length > 0) {
        const queryText = queryLines.join('\n');
        const highlighted = highlightViewsQuery(queryText);
        highlightedLines.push(highlighted);
    }

    return highlightedLines.join('\n');
}

// Function to highlight a query within the views output
function highlightViewsQuery(queryText) {
    // Extract the SQL part from the query definition
    let sqlPart = queryText;

    // Split into lines to handle the structure
    const lines = queryText.split('\n');
    const highlightedLines = [];

    for (const line of lines) {
        if (line.match(/^(table|form)\s*=\s*"/)) {
            // This is the start line
            const match = line.match(/^((table|form)\s*=\s*")(.*)/);
            if (match) {
                const prefix = match[1];
                const sqlStart = match[3];
                highlightedLines.push(
                    `<span class="view-query-type">${escapeHtml(prefix)}</span>` +
                    (sqlStart ? highlightSyntax(sqlStart) : '')
                );
            } else {
                highlightedLines.push(escapeHtml(line));
            }
        } else if (line.trim().endsWith('"')) {
            // This is the end line
            const sqlEnd = line.substring(0, line.lastIndexOf('"'));
            const quote = '"';
            highlightedLines.push(
                (sqlEnd ? highlightSyntax(sqlEnd) : '') +
                `<span class="view-query-type">${escapeHtml(quote)}</span>`
            );
        } else {
            // This is a middle line with SQL content
            if (line.trim()) {
                highlightedLines.push(highlightSyntax(line));
            } else {
                highlightedLines.push(line);
            }
        }
    }

    return highlightedLines.join('\n');
}

// Helper function to escape HTML
function escapeHtml(text) {
    return text.replace(/[<>&"']/g, function(match) {
        return {
            '<': '&lt;',
            '>': '&gt;',
            '&': '&amp;',
            '"': '&quot;',
            "'": '&#39;'
        }[match];
    });
}

// Function for selecting previous queries from dropdown
function selectPreviousQuery() {
    const select = document.getElementById('previousQueries');
    const query = select.value;
    if (query) {
        queryInput.value = query;
        select.selectedIndex = 0;
        updateHighlighting(); // Trigger highlighting update

        if (document.getElementById('autoQuery').checked) {
            executeQuery(false);
        }
    }
}

// Auto-query functionality
function checkAndExecuteQuery() {
    const query = queryInput.value;
    if (query && query !== lastExecutedQuery) {
        // Don't add to history during auto-query
        executeQuery(false);
    }
}

// Handle auto-query checkbox
document.getElementById('autoQuery').addEventListener('change', function() {
    if (this.checked) {
        autoQueryInterval = setInterval(checkAndExecuteQuery, 1000);
    } else {
        if (autoQueryInterval) {
            clearInterval(autoQueryInterval);
            autoQueryInterval = null;
        }
    }
});

// Hide suggestions when clicking outside
document.addEventListener('click', function(e) {
    if (!e.target.closest('.autocomplete-container')) {
        hideAutoCompleteSuggestions();
    }
});

window.addEventListener('load', function() {
    loadAvailableEventTypes();
    loadViewQueries();

    // Insert graph container after controls
    const controlsContainer = document.querySelector('.controls-container');
    controlsContainer.insertAdjacentHTML('afterend', `
        <div class="graph-container" id="graphContainer">
            <div class="graph-controls">
                <label>Graph Controls:</label>
                <button onclick="resetGraphZoom()" style="margin-right: 10px;">Reset Zoom</button>
                <div style="display: inline-flex; align-items: center;">
                    <label style="margin-right: 10px;"><input type="checkbox" id="zeroYAxisCheckbox" onchange="toggleZeroYAxis()"> Zero Y-Axis</label>
                </div>
                <div style="display: inline-flex; align-items: center; margin-left: 10px;">
                    <span style="margin-right: 5px;">Columns:</span>
                    <div class="column-selector" style="display: inline-flex; flex-wrap: wrap; margin: 0;"></div>
                </div>
            </div>
            <div class="graph-canvas-container">
                <canvas id="graphCanvas"></canvas>
                <div class="graph-status">No data to display</div>
                <div class="graph-resize-handle" id="graphResizeHandle"></div>
            </div>
        </div>
    `);

    // Initialize graph elements
    graphContainer = document.getElementById('graphContainer');
    graphCanvas = document.getElementById('graphCanvas');
    
    // Initialize resize functionality
    initGraphResize();
});

// Add these variables at the top with other globals
let isResizing = false;
let initialHeight = 0;
let initialY = 0;

function initGraphResize() {
    const resizeHandle = document.getElementById('graphResizeHandle');
    const graphContainer = document.getElementById('graphContainer');
    const canvasContainer = document.querySelector('.graph-canvas-container');
    
    if (!resizeHandle || !graphContainer || !canvasContainer) return;
    
    // Default height (can be stored in localStorage for persistence)
    const defaultHeight = localStorage.getItem('graphHeight') || 400;
    canvasContainer.style.height = `${defaultHeight}px`;
    
    resizeHandle.addEventListener('mousedown', function(e) {
        isResizing = true;
        initialY = e.clientY;
        initialHeight = canvasContainer.offsetHeight;
        
        // Prevent text selection during resize
        document.body.style.userSelect = 'none';
        document.body.style.cursor = 'ns-resize';
        
        e.preventDefault();
    });
    
    document.addEventListener('mousemove', function(e) {
        if (!isResizing) return;
        
        const deltaY = e.clientY - initialY;
        const newHeight = Math.max(200, initialHeight + deltaY); // Minimum height: 200px
        
        canvasContainer.style.height = `${newHeight}px`;
        
        // Update the chart to fit the new container size
        if (graphChart) {
            graphChart.resize();
        }
        
        e.preventDefault();
    });
    
    document.addEventListener('mouseup', function() {
        if (isResizing) {
            isResizing = false;
            document.body.style.userSelect = '';
            document.body.style.cursor = '';
            
            // Save height preference
            const currentHeight = canvasContainer.offsetHeight;
            localStorage.setItem('graphHeight', currentHeight);
            
            // Final resize of the chart
            if (graphChart) {
                graphChart.resize();
            }
        }
    });
    
    // Also handle cases where mouse leaves the window
    document.addEventListener('mouseleave', function() {
        if (isResizing) {
            isResizing = false;
            document.body.style.userSelect = '';
            document.body.style.cursor = '';
        }
    });
}

// Initialize everything
updatePreviousQueriesDropdown();

// Initialize syntax highlighter
const highlighterElements = createSyntaxHighlighter();
queryInput = highlighterElements.queryInput;
highlightDiv = highlighterElements.highlightDiv;
autocompleteSuggestions = document.getElementById('autocompleteSuggestions');

// Event listeners
queryInput.addEventListener('input', function(e) {
    updateHighlighting();

    const suggestions = getAutoCompleteSuggestions(queryInput.value, queryInput.selectionStart);
    showAutoCompleteSuggestions(suggestions);
});

queryInput.addEventListener('scroll', function() {
    highlightDiv.scrollTop = queryInput.scrollTop;
    highlightDiv.scrollLeft = queryInput.scrollLeft;
});

queryInput.addEventListener('keydown', function(e) {
    if (autocompleteSuggestions && autocompleteSuggestions.style.display === 'block') {
        if (e.key === 'ArrowDown') {
            e.preventDefault();
            navigateSuggestions(1);
            return;
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            navigateSuggestions(-1);
            return;
        } else if (e.key === 'Tab') {
            e.preventDefault();
            if (currentSuggestions.length > 0) {
                if (selectedSuggestionIndex === -1) {
                    // If no suggestion is selected, select the first one
                    selectedSuggestionIndex = 0;
                    const firstSuggestion = autocompleteSuggestions.children[0];
                    if (firstSuggestion) {
                        firstSuggestion.classList.add('selected');
                    }
                } else {
                    // Tab cycles forward, Shift+Tab cycles backward
                    navigateSuggestions(e.shiftKey ? -1 : 1);
                }
            }
            return;
        } else if (e.key === 'Enter' && selectedSuggestionIndex >= 0) {
            e.preventDefault();
            applySuggestion(currentSuggestions[selectedSuggestionIndex]);
            return;
        } else if (e.key === 'Escape') {
            hideAutoCompleteSuggestions();
            return;
        }
    }

    if ((e.key === 'Enter' || e.keyCode === 13) && (e.ctrlKey || e.metaKey)) {
        console.log('Ctrl+Enter pressed, executing query...');
        // Hide autocompletion before executing
        hideAutoCompleteSuggestions();
        executeQuery(true);
        e.preventDefault();
        return false;
    }
});

// Initial highlighting
updateHighlighting();