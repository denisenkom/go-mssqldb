package krb5

import (
	"os"
	"strings"
	"testing"

	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestReadKrb5ConfigHappyPath(t *testing.T) {
	tests := []struct {
		name      string
		cfg       msdsn.Config
		validate  func(t testing.TB, cfg msdsn.Config, actual *krb5Login)
		confPath  string
		tabPath   string
		cachePath string
	}{
		{
			name: "basic match",
			cfg: msdsn.Config{
				User:      "username",
				Password:  "placeholderpassword",
				ServerSPN: "serverspn",
				Parameters: map[string]string{
					"krb5-configfile":         "krb5-configfile",
					"krb5-keytabfile":         "krb5-keytabfile",
					"krb5-credcachefile":      "krb5-credcachefile",
					"krb5-realm":              "krb5-realm",
					"krb5-dnslookupkdc":       "false",
					"krb5-udppreferencelimit": "1234",
				},
			},
			validate: basicConfigMatch,
		},
		{
			name: "realm in user name",
			cfg: msdsn.Config{
				User:      "username@realm.com",
				Password:  "placeholderpassword",
				ServerSPN: "serverspn",
				Parameters: map[string]string{
					"krb5-configfile":         "krb5-configfile",
					"krb5-keytabfile":         "krb5-keytabfile",
					"krb5-credcachefile":      "krb5-credcachefile",
					"krb5-dnslookupkdc":       "false",
					"krb5-udppreferencelimit": "1234",
				},
			},
			validate: func(t testing.TB, cfg msdsn.Config, actual *krb5Login) {
				if actual.Realm != "realm.com" {
					t.Errorf("Realm should have been copied from user name. Got: %s", actual.Realm)
				}
				if actual.UserName != "username" {
					t.Errorf("UserName shouldn't include the realm. Got: %s", actual.UserName)
				}
			},
		},
		{
			name: "using defaults for file paths",
			cfg: msdsn.Config{
				User:      "username",
				Password:  "",
				ServerSPN: "serverspn",
				Parameters: map[string]string{
					"krb5-dnslookupkdc":       "false",
					"krb5-udppreferencelimit": "1234",
					"krb5-realm":              "krb5-realm",
				},
			},
			validate: func(t testing.TB, cfg msdsn.Config, actual *krb5Login) {
				if actual.Krb5ConfigFile != `/etc/krb5.conf` {
					t.Errorf("Expected default conf file path. Got: %s", actual.Krb5ConfigFile)
				}
				if actual.KeytabFile != `/etc/krb5.keytab` {
					t.Errorf("Expecte keytab path from libdefaults. Got %s", actual.KeytabFile)
				}
			},
		},
		{
			name:      "Using environment variables",
			confPath:  `/etc/my.config`,
			cachePath: `/tmp/mycache`,
			tabPath:   `/tmp/mytab`,
			cfg: msdsn.Config{
				User:      "username",
				Password:  "",
				ServerSPN: "serverspn",
				Parameters: map[string]string{
					"krb5-dnslookupkdc":       "false",
					"krb5-udppreferencelimit": "1234",
					"krb5-realm":              "krb5-realm",
				},
			},
			validate: func(t testing.TB, cfg msdsn.Config, actual *krb5Login) {
				if actual.Krb5ConfigFile != `/etc/my.config` {
					t.Errorf("Expected conf file path from env var. Got: %s", actual.Krb5ConfigFile)
				}
				if actual.KeytabFile != `/tmp/mytab` {
					t.Errorf("Expected tab file from env var. Got: %s", actual.KeytabFile)
				}
				if actual.CredCacheFile != `/tmp/mycache` {
					t.Errorf("Expected cache file from env var. Got: %s", actual.CredCacheFile)
				}
			},
		},
		{
			name:      "no keytab from environment when user name is unset",
			confPath:  `/etc/my.config`,
			cachePath: `/tmp/mycache`,
			tabPath:   `/tmp/mytab`,
			cfg: msdsn.Config{
				User:      "",
				Password:  "",
				ServerSPN: "serverspn",
				Parameters: map[string]string{
					"krb5-dnslookupkdc":       "false",
					"krb5-udppreferencelimit": "1234",
					"krb5-realm":              "krb5-realm",
				},
			},
			validate: func(t testing.TB, cfg msdsn.Config, actual *krb5Login) {
				if actual.Krb5ConfigFile != `/etc/my.config` {
					t.Errorf("Expected conf file path from env var. Got: %s", actual.Krb5ConfigFile)
				}
				if actual.KeytabFile != "" {
					t.Errorf("Expected no tab file. Got: %s", actual.KeytabFile)
				}
				if actual.CredCacheFile != `/tmp/mycache` {
					t.Errorf("Expected cache file from env var. Got: %s", actual.CredCacheFile)
				}
			},
		},
	}
	revert := mockFileExists()
	defer revert()

	revertConfig := mockDefaultConfig()
	defer revertConfig()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if len(test.cachePath) > 0 {
				cp := os.Getenv("KRB5CCNAME")
				os.Setenv("KRB5CCNAME", test.cachePath)
				defer os.Setenv("KRB5CCNAME", cp)
			}
			if len(test.confPath) > 0 {
				cp := os.Getenv("KRB5_CONFIG")
				os.Setenv("KRB5_CONFIG", test.confPath)
				defer os.Setenv("KRB5_CONFIG", cp)
			}
			if len(test.tabPath) > 0 {
				cp := os.Getenv("KRB5_KTNAME")
				os.Setenv("KRB5_KTNAME", test.tabPath)
				defer os.Setenv("KRB5_KTNAME", cp)
			}

			actual, err := readKrb5Config(test.cfg)

			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			test.validate(t, test.cfg, actual)
		})

	}
}

func basicConfigMatch(t testing.TB, config msdsn.Config, actual *krb5Login) {
	if actual.Krb5ConfigFile != config.Parameters[keytabConfigFile] {
		t.Errorf("Expected Krb5ConfigFile %v, found %v", config.Parameters[keytabConfigFile], actual.Krb5ConfigFile)
	}

	if actual.KeytabFile != config.Parameters[keytabFile] {
		t.Errorf("Expected KeytabFile %v, found %v", config.Parameters[keytabFile], actual.KeytabFile)
	}

	if actual.CredCacheFile != config.Parameters[credCacheFile] {
		t.Errorf("Expected CredCacheFile %v, found %v", config.Parameters[credCacheFile], actual.CredCacheFile)
	}

	if actual.Realm != config.Parameters[realm] {
		t.Errorf("Expected Realm %v, found %v", config.Parameters[realm], actual.Realm)
	}

	if actual.UserName != config.User {
		t.Errorf("Expected username %v, found %v", config.User, actual.UserName)
	}

	if actual.Password != config.Password {
		t.Errorf("Expected password %v, found %v", config.Password, actual.Password)
	}

	if actual.ServerSPN != config.ServerSPN {
		t.Errorf("Expected serverSpn %v, found %v", config.ServerSPN, actual.ServerSPN)
	}

	if actual.DNSLookupKDC != false {
		t.Errorf("Expected DNSLookupKDC %v, found %v", false, actual.DNSLookupKDC)
	}

	if actual.UDPPreferenceLimit != 1234 {
		t.Errorf("Expected UDPPreferenceLimit %v, found %v", 1234, actual.UDPPreferenceLimit)
	}
}
func TestReadKrb5ConfigErrorCases(t *testing.T) {

	tests := []struct {
		name               string
		dnslookup          string
		udpPreferenceLimit string
		expectedError      string
	}{

		{
			name:               "invalid dnslookupkdc",
			dnslookup:          "a",
			udpPreferenceLimit: "1234",
			expectedError:      "invalid 'krb5-dnslookupkdc' parameter 'a': strconv.ParseBool: parsing \"a\": invalid syntax",
		},
		{
			name:               "invalid udpPreferenceLimit",
			dnslookup:          "true",
			udpPreferenceLimit: "a",
			expectedError:      "invalid 'krb5-udppreferencelimit' parameter 'a': strconv.Atoi: parsing \"a\": invalid syntax",
		},
	}

	revertConfig := mockDefaultConfig()
	defer revertConfig()

	for _, tt := range tests {
		config := msdsn.Config{
			Parameters: map[string]string{
				"krb5-dnslookupkdc":       tt.dnslookup,
				"krb5-udppreferencelimit": tt.udpPreferenceLimit,
			},
		}

		actual, err := readKrb5Config(config)

		if actual != nil {
			t.Errorf("Unexpected return value expected nil, found %v", actual)
			continue
		}

		if err == nil {
			t.Errorf("Expected error '%v', found nil", tt.expectedError)
			continue
		}

		if err.Error() != tt.expectedError {
			t.Errorf("Expected error %v, found %v", tt.expectedError, err)
		}
	}
}

func TestReadKrb5ConfigGetsDefaultsFromConfFile(t *testing.T) {
	loadDefaultConfigFromFile = func(krb5Login *krb5Login) (*config.Config, error) {
		c := config.New()
		c.LibDefaults.DefaultRealm = "myrealm"
		c.LibDefaults.DefaultClientKeytabName = "mykeytabexists"
		c.LibDefaults.DNSLookupKDC = krb5Login.DNSLookupKDC
		c.LibDefaults.UDPPreferenceLimit = krb5Login.UDPPreferenceLimit
		return c, nil
	}
	defer func() {
		loadDefaultConfigFromFile = newKrb5ConfigFromFile
	}()
	revert := mockFileExists()
	defer revert()

	cfg := msdsn.Config{
		User:      "username",
		Password:  "",
		ServerSPN: "serverspn",
		Parameters: map[string]string{
			"krb5-dnslookupkdc":       "false",
			"krb5-udppreferencelimit": "1234",
		},
	}
	login, err := readKrb5Config(cfg)
	if err != nil {
		t.Errorf("Unexpected error from readKrb5Config %s", err.Error())
	}
	if login.Realm != "myrealm" {
		t.Errorf("Unexpected realm. Got %s", login.Realm)
	}
	if login.KeytabFile != "mykeytabexists" {
		t.Errorf("Unexpected keytab file. Got: %s", login.KeytabFile)
	}

}
func TestValidateKrb5LoginParams(t *testing.T) {

	tests := []struct {
		name                string
		input               *krb5Login
		expectedLoginMethod loginMethod
		expectedError       error
	}{

		{
			name: "happy username and password",
			input: &krb5Login{
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: usernameAndPassword,
			expectedError:       nil,
		},
		{
			name: "username and password, missing realm",
			input: &krb5Login{
				Krb5ConfigFile: "exists",
				Realm:          "",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: none,
			expectedError:       ErrRealmRequiredWithUsernameAndPassword,
		},
		{
			name: "username and password, missing Krb5ConfigFile",
			input: &krb5Login{
				Krb5ConfigFile: "",
				Realm:          "realm",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileRequiredWithUsernameAndPassword,
		},
		{
			name: "username and password, Krb5ConfigFile file not found",
			input: &krb5Login{
				Krb5ConfigFile: "missing",
				Realm:          "realm",
				UserName:       "username",
				Password:       "password",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileDoesNotExist,
		},
		{
			name: "happy keytab",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: keyTabFile,
			expectedError:       nil,
		},
		{
			name: "keytab, missing username",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "",
			},
			expectedLoginMethod: none,
			expectedError:       ErrUsernameRequiredWithKeytab,
		},
		{
			name: "keytab, missing realm",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "exists",
				Realm:          "",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrRealmRequiredWithKeytab,
		},
		{
			name: "keytab, missing Krb5ConfigFile",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileRequiredWithKeytab,
		},
		{
			name: "keytab, Krb5ConfigFile file not found",
			input: &krb5Login{
				KeytabFile:     "exists",
				Krb5ConfigFile: "missing",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileDoesNotExist,
		},
		{
			name: "keytab, KeytabFile file not found",
			input: &krb5Login{
				KeytabFile:     "missing",
				Krb5ConfigFile: "exists",
				Realm:          "realm",
				UserName:       "username",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKeytabFileDoesNotExist,
		},
		{
			name: "happy credential cache",
			input: &krb5Login{
				CredCacheFile:  "exists",
				Krb5ConfigFile: "exists",
			},
			expectedLoginMethod: cachedCredentialsFile,
			expectedError:       nil,
		},
		{
			name: "credential cache, missing Krb5ConfigFile",
			input: &krb5Login{
				CredCacheFile:  "exists",
				Krb5ConfigFile: "",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileRequiredWithCredCache,
		},
		{
			name: "credential cache, Krb5ConfigFile file not found",
			input: &krb5Login{
				CredCacheFile:  "exists",
				Krb5ConfigFile: "missing",
			},
			expectedLoginMethod: none,
			expectedError:       ErrKrb5ConfigFileDoesNotExist,
		},
		{
			name: "credential cache, CredCacheFile file not found",
			input: &krb5Login{
				CredCacheFile:  "missing",
				Krb5ConfigFile: "exists",
			},
			expectedLoginMethod: none,
			expectedError:       ErrCredCacheFileDoesNotExist,
		},
		{
			name:                "no login method match",
			input:               &krb5Login{},
			expectedLoginMethod: none,
			expectedError:       ErrRequiredParametersMissing,
		},
	}

	revert := mockFileExists()
	defer revert()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.input.loginMethod = none
			err := validateKrb5LoginParams(tt.input)

			if err != nil && tt.expectedError == nil {
				t.Errorf("Unexpected error %v, expected nil", err)
			}

			if err == nil && tt.expectedError != nil {
				t.Errorf("Expected error %v, found nil", tt.expectedError)
			}

			if err != tt.expectedError {
				t.Errorf("Expected error %v, found %v", tt.expectedError, err)
			}

			if tt.input.loginMethod != tt.expectedLoginMethod {
				t.Errorf("Expected loginMethod %v, found %v", tt.expectedLoginMethod, tt.input.loginMethod)
			}
		})
	}
}

func mockFileExists() func() {
	fileExists = func(filename string, errWhenFileNotFound error) (bool, error) {
		if strings.Contains(filename, "exists") || filename == `/etc/krb5.keytab` {
			return true, nil
		}

		return false, errWhenFileNotFound
	}

	return func() { fileExists = fileExistsOS }
}

func mockDefaultConfig() func() {
	loadDefaultConfigFromFile = func(krb5Login *krb5Login) (*config.Config, error) {
		return config.New(), nil
	}
	return func() {
		loadDefaultConfigFromFile = newKrb5ConfigFromFile
	}
}

func TestGetAuth(t *testing.T) {
	config := msdsn.Config{
		User:      "username",
		Password:  "password",
		ServerSPN: "serverspn",
		Parameters: map[string]string{
			"krb5-configfile":         "exists",
			"krb5-keytabfile":         "exists",
			"krb5-keytabcachefile":    "exists",
			"krb5-realm":              "krb5-realm",
			"krb5-dnslookupkdc":       "false",
			"krb5-udppreferencelimit": "1234",
		},
	}

	revert := mockFileExists()
	defer revert()
	revertConfig := mockDefaultConfig()
	defer revertConfig()

	a, err := getAuth(config)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}

	actual := a.(*krbAuth)

	if actual.krb5Config.Krb5ConfigFile != config.Parameters[keytabConfigFile] {
		t.Errorf("Expected Krb5ConfigFile %v, found %v", config.Parameters[keytabConfigFile], actual.krb5Config.Krb5ConfigFile)
	}

	if actual.krb5Config.KeytabFile != config.Parameters[keytabFile] {
		t.Errorf("Expected KeytabFile %v, found %v", config.Parameters[keytabFile], actual.krb5Config.KeytabFile)
	}

	if actual.krb5Config.CredCacheFile != config.Parameters[credCacheFile] {
		t.Errorf("Expected CredCacheFile %v, found %v", config.Parameters[credCacheFile], actual.krb5Config.CredCacheFile)
	}

	if actual.krb5Config.Realm != config.Parameters[realm] {
		t.Errorf("Expected Realm %v, found %v", config.Parameters[realm], actual.krb5Config.Realm)
	}

	if actual.krb5Config.UserName != config.User {
		t.Errorf("Expected username %v, found %v", config.User, actual.krb5Config.UserName)
	}

	if actual.krb5Config.Password != config.Password {
		t.Errorf("Expected password %v, found %v", config.Password, actual.krb5Config.Password)
	}

	if actual.krb5Config.ServerSPN != config.ServerSPN {
		t.Errorf("Expected serverSpn %v, found %v", config.ServerSPN, actual.krb5Config.ServerSPN)
	}

	if actual.krb5Config.DNSLookupKDC != false {
		t.Errorf("Expected DNSLookupKDC %v, found %v", false, actual.krb5Config.DNSLookupKDC)
	}

	if actual.krb5Config.UDPPreferenceLimit != 1234 {
		t.Errorf("Expected UDPPreferenceLimit %v, found %v", 1234, actual.krb5Config.UDPPreferenceLimit)
	}
}
