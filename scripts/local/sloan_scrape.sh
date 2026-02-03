#!/bin/bash
# Scrape Sloan Foundation grants using agent-browser
# Output: /tmp/sloan_grants.json

OUTPUT_FILE="/tmp/sloan_grants.json"
TOTAL_PAGES=68  # 3358 grants / 50 per page

echo "[Start] $(date)"
echo "[]" > "$OUTPUT_FILE"

# Set 50 per page
echo "[Config] Setting 50 per page..."
npx agent-browser select 'select:has(option:text("50"))' "50" 2>/dev/null
sleep 3

for PAGE in $(seq 1 $TOTAL_PAGES); do
    echo "[Page $PAGE/$TOTAL_PAGES] Scraping..."

    # Navigate to page
    if [ $PAGE -gt 1 ]; then
        npx agent-browser open "https://sloan.org/grants-database?page=$PAGE" 2>/dev/null
        sleep 3
    fi

    # Extract grants via JavaScript
    GRANTS=$(npx agent-browser eval "
        const grants = [];
        document.querySelectorAll('main ul > li, main ol > li').forEach(li => {
            const text = li.innerText;
            if (text.includes('\$') && /20\\\\d{2}/.test(text)) {
                const lines = text.split('\\\\n').map(l => l.trim()).filter(l => l);
                const permalink = li.querySelector('a[href*=\"grant-detail\"]');

                // Parse first line: Grantee Amount City, State Year
                const firstLine = lines[0] || '';
                const amountMatch = firstLine.match(/\\$([0-9,]+)/);
                const yearMatch = firstLine.match(/\\b(20\\\\d{2})\\b/);
                const locationMatch = firstLine.match(/\\$[0-9,]+\\s+(.+?)\\s+20\\\\d{2}/);
                const granteeMatch = firstLine.match(/^(.+?)\\s*\\$/);

                // Parse metadata lines
                let description = '', program = '', subProgram = '', initiative = '', investigator = '';
                let foundProgram = false;
                for (let i = 1; i < lines.length; i++) {
                    const line = lines[i];
                    if (line === 'PROGRAM') { foundProgram = true; continue; }
                    if (line === 'SUB-PROGRAM') { foundProgram = false; continue; }
                    if (line === 'INITIATIVE') { foundProgram = false; continue; }
                    if (line === 'INVESTIGATOR') { foundProgram = false; continue; }
                    if (line === 'PERMALINK' || line === 'CLOSE' || line === 'MORE') break;

                    if (lines[i-1] === 'PROGRAM') program = line;
                    else if (lines[i-1] === 'SUB-PROGRAM') subProgram = line;
                    else if (lines[i-1] === 'INITIATIVE') initiative = line;
                    else if (lines[i-1] === 'INVESTIGATOR') investigator = line;
                    else if (!description && !['PROGRAM','SUB-PROGRAM','INITIATIVE','INVESTIGATOR'].includes(line)) {
                        description = line;
                    }
                }

                // Parse location
                let city = '', state = '', country = '';
                if (locationMatch) {
                    const loc = locationMatch[1].trim();
                    if (loc.includes(', ')) {
                        const parts = loc.split(', ');
                        city = parts.slice(0, -1).join(', ');
                        const last = parts[parts.length - 1];
                        if (last.length === 2 && last === last.toUpperCase()) {
                            state = last;
                            country = 'United States';
                        } else {
                            country = last;
                        }
                    } else {
                        city = loc;
                    }
                }

                grants.push({
                    grant_id: permalink ? permalink.href.match(/g-(\\\\d{4}-\\\\d+)/)?.[1] : null,
                    grantee: granteeMatch ? granteeMatch[1].trim() : null,
                    amount: amountMatch ? parseFloat(amountMatch[1].replace(/,/g, '')) : null,
                    city: city || null,
                    state: state || null,
                    country: country || null,
                    year: yearMatch ? parseInt(yearMatch[1]) : null,
                    description: description || null,
                    program: program || null,
                    sub_program: subProgram || null,
                    initiative: initiative || null,
                    investigator: investigator || null
                });
            }
        });
        JSON.stringify(grants);
    " 2>/dev/null)

    # Append to output file
    if [ -n "$GRANTS" ] && [ "$GRANTS" != "[]" ]; then
        # Merge arrays
        python3 -c "
import json
import sys

existing = json.load(open('$OUTPUT_FILE'))
new = json.loads(sys.argv[1])
existing.extend(new)
json.dump(existing, open('$OUTPUT_FILE', 'w'))
print(f'[Page $PAGE] Added {len(new)} grants, total: {len(existing)}')
" "$GRANTS"
    fi

    sleep 1
done

echo "[Done] $(date)"
echo "[Output] $OUTPUT_FILE"
