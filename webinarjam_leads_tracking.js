(async function() { // IIFE is now async to allow 'await' inside
    const queryParams = new URLSearchParams(window.location.search);

    const email = queryParams.get('wj_lead_email');
    const wjFirstNameInput = queryParams.get('wj_lead_first_name') || '';
    const wjLastNameInput = queryParams.get('wj_lead_last_name') || '';   
    
    const phoneCountryCode = queryParams.get('wj_lead_phone_country_code');
    const phoneNumber = queryParams.get('wj_lead_phone_number');
    
    // --- Name Splitting Logic ---
    let finalFirstName = '';
    let finalLastName = '';

    if (wjLastNameInput.trim() !== '') {
        finalFirstName = wjFirstNameInput.trim();
        finalLastName = wjLastNameInput.trim();
    } else if (wjFirstNameInput.trim() !== '') {
        const nameParts = wjFirstNameInput.trim().split(/\s+/); 
        finalFirstName = nameParts[0] || '';
        if (nameParts.length > 1) {
            finalLastName = nameParts.slice(1).join(' ').trim();
        }
    }
    // --- End of Name Splitting Logic ---

    // --- Phone Number and Country Derivation ---
    let fullPhoneNumber = '';
    if (phoneCountryCode && phoneNumber) {
        fullPhoneNumber = phoneCountryCode.trim() + phoneNumber.trim();
    } else if (phoneNumber) {
        fullPhoneNumber = phoneNumber.trim();
    }

    let derivedCountryISO = '';
    if (phoneCountryCode) {
        const countryCodeToISOMap = {
            "+1": "US", "+44": "GB", "+45": "DK", "+46": "SE", "+47": "NO",
            "+49": "DE", "+358": "FI", "+33": "FR", "+34": "ES", "+39": "IT",
            "+31": "NL", "+61": "AU", "+43": "AT", "+48": "PL", 
            "+41": "CH", "+32": "BE", "+30": "GR", "+353": "IE", 
            "+351": "PT", "+420": "CZ"
            // Add more as needed
        };
        derivedCountryISO = countryCodeToISOMap[phoneCountryCode.trim()] || ''; 
    }
    // --- End of Phone Number and Country Derivation ---

    if (email) {
        const payloadToSend = {
            email: email,
            firstName: finalFirstName || '',
            lastName: finalLastName || '',
            phone: fullPhoneNumber || '',
            country: derivedCountryISO || '',
            tags: ['webinar-registered-js-prod-v2', 'source-webinarjam-typage'], // Updated tag example
            source: 'WebinarJam Thank You Page (JS Tracking v2)' // Updated source example
            // customFields: [], // Define and populate if needed
        };
        
        // if (payloadToSend.customFields && payloadToSend.customFields.length === 0) {
        //     delete payloadToSend.customFields;
        // }

        const psfWebhookUrl = 'https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/f46d6bca-22d3-470f-a558-ccabd47b7494';
        
        const MAX_ATTEMPTS = 3;
        const RETRY_DELAY_MS = 2000; // 2 seconds

        async function sendWebhookWithRetries() {
            for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                try {
                    const response = await fetch(psfWebhookUrl, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(payloadToSend),
                    });

                    if (response.ok) {
                        // Success, no need for further action in this silent version
                        // If you needed the response data: await response.json().catch(() => ({}));
                        return true; // Indicate success
                    } else {
                        // Response not OK
                        if (attempt === MAX_ATTEMPTS) {
                             // Last attempt failed, could log this to an external service
                            return false; // Indicate final failure
                        }
                        // Not the last attempt, wait before retrying
                        // Fall through to the delay
                    }
                } catch (error) {
                    // Network error or other fetch-related exception
                    if (attempt === MAX_ATTEMPTS) {
                        // Last attempt failed due to network error
                        return false; // Indicate final failure
                    }
                    // Not the last attempt, wait before retrying
                    // Fall through to the delay
                }
                
                // If not successful and not the last attempt, wait
                if (attempt < MAX_ATTEMPTS) {
                    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
                }
            }
            return false; // Should only be reached if all attempts fail
        }

        await sendWebhookWithRetries(); // Call the function to send with retries
    }
  })();