%>theta holds variable coefficient values (scaled)
%>theta_norm contains scaling factor for each of the coefficients SEE PAPER
%>coef_map contains indices of base_coef concatenated vector into which
%elements of theta will be placed
%>base_coef is concatenated vector of all coefficients for 3,2 hub, 2, and 1 player
%markets
%>loss_metric is one if minimizing MAPE, 2 if minimizing R^2, 
%carriers holds data on each of the carriers, to which new coefficients%
%will be added (use a base file that will be read in)
%>market_data_mat: for each, need market size, market rank,  Mnew, Cnew,  market type start and end indices in base coef. sorted by
%carrier and then by market, and indices of transformed coefficients in the transformed coefficient vector 
%>fixed_carrier: vector of which carriers will have their frequencies set to
%empirical frequencies
%>fixed_market_carriers: vector of indices of carriers with markets to be
%fixed
%>fixed_markets: cell array wich markets are fixed for a carrier, i.e.
%index of market in carrier.Market_freqs
%record old and new coefficients, and make sure to send new frequency
%PROBABLY NEED OTHER GENERAL SCENARIO INFORMATION MINED FROM FILE NOT IN
%CARRIER DATA STRUCTURE
function loss = network_game_loss_quadratic(theta,theta_norm,coef_map,base_coef,loss_metric,carriers,market_data_mat,fixed_carrier,fixed_market_carriers,fixed_markets,num_carriers,segment_competitors,Market_freqs,empirical_freqs,file_write,MAPE_incl,outfile_fn)
    %MAPE_incl(1)=0;
    %MAPE_incl(8)=0; %US LAS_PDX
    %MAPE_incl(9)=0; %WN LAS_PHX
    %MAPE_incl(23)=0; %WN LAX_OAK
    %MAPE_incl(43)=0; %WN OAK_SAN
    % 3 player interaction leave-out index mapping
    remove_interaction = [3,2,1];
    %%%PART I: use new theta values to recompute profit function
    %%%coefficient for all carriers

    %%%unnormalize theta
    theta_scaled = theta.*theta_norm;
    %%%insert back into bases
    %%this won't work, as some theta's must be duplicated: base_coef(coef_map)=theta_scaled;  
    for i=1:numel(coef_map)
        base_coef(coef_map{i})=theta_scaled(i);
    end
    %%%replace carriers.coef with new adjusted coefficients
    Mold = 1000;
    Cold = 10000;
    market_data_ind = 0;
    for carrier_ind=1:numel(carriers)
       %initialize new coefficients 
       new_coef = zeros(1,numel(carriers{carrier_ind}.coef));
       for market_ind=1:numel(carriers{carrier_ind}.Markets)
           market_data_ind = market_data_ind + 1;
           
           %insert new adjusted coefficients for appropriate market size
           %from base coef into appropriate position in new coef            
           mkt_size = market_data_mat(market_data_ind,1);
           freq_ind =market_data_mat(market_data_ind,2);
           Mnew = market_data_mat(market_data_ind,3);
           Cnew = market_data_mat(market_data_ind,4);
           %start and end indices of the base coefficients to use for this
           %carrier/market combo
           basecoef_start =  market_data_mat(market_data_ind,5);
           basecoef_end = market_data_mat(market_data_ind,6);
           %get base coefficients to use 
           base = base_coef(basecoef_start:basecoef_end);
           %indices of these coefficients in full concatenated vector of
           %all market coefficients for the carrier
           transcoef_start = market_data_mat(market_data_ind,7);
           transcoef_end= market_data_mat(market_data_ind,8);          
           %create transformed coefficients with new market size and cost
           %in the correct order using specified base coefficients           
           if (mkt_size==1)
               transcoef = [-(Mnew/Mold)*base(1),(Mnew/Mold)*(Cold-base(2))-Cnew,-(Mnew/Mold)*base(3) ];
           elseif (mkt_size==2)
               transcoef = zeros(1,6);
               %intercept
               transcoef(1) = -(Mnew/Mold)*base(1);
               % linear fs 
               for i = 1:2
                   if (freq_ind == i)
                       transcoef(i+1)=(Mnew/Mold)*(Cold-base(2))-Cnew;
                   else
                       transcoef(i+1) = -(Mnew/Mold)*base(3);
                   end
               end
               %quadtratic fs
               for i = 1:2
                   if (freq_ind == i)
                       transcoef(i+3)=-(Mnew/Mold)*base(4);
                   else
                       transcoef(i+3) = -(Mnew/Mold)*base(5);
                   end
               end
               %interation of fs
               transcoef(6)=-(Mnew/Mold)*base(6);           
           elseif  (mkt_size==3)  
               transcoef = zeros(1,10);
               %intercept
               transcoef(1) = -(Mnew/Mold)*base(1);                
               % linear fs 
               for i = 1:3
                   if (freq_ind == i)
                       transcoef(i+1)=(Mnew/Mold)*(Cold-base(2))-Cnew;
                   else
                       transcoef(i+1) = -(Mnew/Mold)*base(3);
                   end
               end
               %quadtratic fs
               for i = 1:3
                   if (freq_ind == i)
                       transcoef(i+4)=-(Mnew/Mold)*base(5);
                   else
                       transcoef(i+4) = -(Mnew/Mold)*base(6);
                   end
               end
               %interaction fs
               if (freq_ind == 1)
                   transcoef(8:10)=[-(Mnew/Mold)*base(8),-(Mnew/Mold)*base(8),-(Mnew/Mold)*base(10)];
               elseif (freq_ind == 2)
                   transcoef(8:10)=[-(Mnew/Mold)*base(8),-(Mnew/Mold)*base(10),-(Mnew/Mold)*base(8)];
               elseif (freq_ind == 3)
                   transcoef(8:10)=[-(Mnew/Mold)*base(10),-(Mnew/Mold)*base(8),-(Mnew/Mold)*base(8)];
               else
                   error('freq ind error')
               end            
           else
               error('market size error')
           end
           %insert new coefficients for this market into vector
           new_coef(transcoef_start:transcoef_end)=transcoef;
       end
       %replace old coefficients in carrier data structure
       carriers{carrier_ind}.coef = new_coef;       
    end
    
    
    
    %%%PART II: run network game using carrier data modified above and
    %%%return frequency estimates
    %set best response and optimization parameters 
    eps=0.1;
    diffs = ones(num_carriers,1) +eps;
    options = optimset('Display', 'off') ;
    
    %myopic best response: each carrier decides frequencies for all the
    %segments that it is competing on
    loop=1;
    while (sum(diffs(fixed_carrier == 0)>eps)>0) %loop until converges (just for non fixed diff indices)
        for carrier_ind=1:num_carriers
            %if carrier is not fixed, run optimization    
            
            if (fixed_carrier(carrier_ind) ==0)    
                
                CALcarrier = carriers{carrier_ind};
                %current frequencies of carrier on all of its market segments
                current_markets = Market_freqs(CALcarrier.Markets);
                f_i = zeros(numel(CALcarrier.Markets),1);
                %loop through markets, get frequency of carrier at these markets
                for i=1:numel(CALcarrier.Markets)
                    current_market_freqs = current_markets{i};
                    %get frequency of current carrier corresponding to current
                    %market
                    f_i(i) = current_market_freqs(CALcarrier.freq_inds(i));      
                end
                %loop through markets, get frequency of carrier at these markets
                %for each market, get linear coefficients and quadratic
                %coefficients
                quad_terms = zeros(numel(CALcarrier.Markets),1);
                lin_terms = zeros(numel(CALcarrier.Markets),1);
                coef_vector_index = 1;
                for i=1:numel(CALcarrier.Markets)
                    freq_ind = CALcarrier.freq_inds(i); 
                    %current_mkt_freqs = current_markets{i};
                    %num_players = numel(current_mkt_freqs);
                    current_market_freqs = current_markets{i};
                    num_players = numel(current_market_freqs);
                    if (num_players)==1
                        num_coefs = 3;
                        current_coefs = -CALcarrier.coef(coef_vector_index:(coef_vector_index + num_coefs - 1));
                        lin_terms(i) = current_coefs(2);
                        quad_terms(i) = current_coefs(3);
                    elseif (num_players)==2
                        num_coefs = 6;
                        current_coefs = -CALcarrier.coef(coef_vector_index:(coef_vector_index + num_coefs - 1));
                        %to create linear coefficient, set freq of current
                        %carrier in current market to 1
                        current_market_freqs(freq_ind) = 1;
                        lin_terms(i) = current_coefs(1+freq_ind) + current_coefs(6)*prod(current_market_freqs);
                        quad_terms(i) = current_coefs(3 + freq_ind);
                    else
                        num_coefs = 10;
                        current_coefs = -CALcarrier.coef(coef_vector_index:(coef_vector_index + num_coefs - 1));
                        current_market_freqs(freq_ind) = 1;
                        %interaction coefficients
                        int_coefs  = current_coefs(8:10);
                        int_coefs(remove_interaction(freq_ind)) = 0;                        
                        lin_terms(i) = current_coefs(1+freq_ind) + int_coefs(1)*current_market_freqs(1)*current_market_freqs(2) + int_coefs(2)*current_market_freqs(1)*current_market_freqs(3) + int_coefs(3)*current_market_freqs(2)*current_market_freqs(3);
                        quad_terms(i) = current_coefs(4 + freq_ind);
                    end                          
                    coef_vector_index = coef_vector_index + num_coefs;         
                   
                end
                %construct Hessian matrix
                H = diag(2*quad_terms);
                
                %optimize frequencies of this carrier for profit
                if any(fixed_market_carriers==carrier_ind)
                    %fix relevant markets for this carrier if applicable      
                    fixed_freqs = CALcarrier.emp_freqs(fixed_markets{carrier_ind});
                    lower_bound = ones(numel(CALcarrier.Markets),1)*.5;
                    lower_bound(fixed_markets{carrier_ind}) = fixed_freqs;
                    upper_bound = ones(numel(CALcarrier.Markets),1)*inf;
             
                    upper_bound(fixed_markets{carrier_ind}) = fixed_freqs;
                    x_i = quadprog(H,lin_terms,CALcarrier.A,CALcarrier.b,[],[],lower_bound,upper_bound,[],options);
                else                    
                    x_i = quadprog(H,lin_terms,CALcarrier.A,CALcarrier.b,[],[],zeros(numel(CALcarrier.Markets),1),ones(numel(CALcarrier.Markets),1)*inf,[],options);
                    
                end                
                %check for convergence
                diffs(carrier_ind)=sum(abs(f_i-x_i));
                %set new optimal frequencies into market frequencies data structure
                for i=1:numel(CALcarrier.Markets)
                    %frequency in market i 
                    new_market_freq = x_i(i);
                    %get current market frequencies
                    current_market_freqs = current_markets{i};
                    %update frequency of current carrier in current market
                    current_market_freqs(CALcarrier.freq_inds(i))= new_market_freq;
                    %put new frequencies in market back into book keeping  market
                    %frequencies data structure
                    Market_freqs{CALcarrier.Markets(i)}=current_market_freqs;                  
                end 
                %save current frequencies and profits for each market for this
                %carrier
                CALcarrier.freqs = x_i;
                % ADD THIS AS A CALCULATION LATER carrier.profits = profit;
            end
        end
        %display loop number and time
        %%%display(loop)
        loop =  loop +1;
        %%%toc
    end
    %final frequencies will be contained in Market freqs and carrier.freqs
    %(organized by market and carrier, respectively)
    %corresponding profits are in carrier.profits
    
    %if a final eval, write results to file: 
    %write market freqs into a reasonable matrix, columns as market, rank, and    
    %frequency, to be matched with sorted dataframe t100ranked in python
    if (file_write ==1)
        freq_results_mat = zeros(sum(segment_competitors),3);
        %row index
        row_ind =1;
        %for each market...
        for mk=1:numel(segment_competitors)
            market_data = Market_freqs{mk};
            %for each competitor in market...
            rk=1;
            for row=row_ind:row_ind+segment_competitors(mk) - 1
                freq_results_mat(row,:)=[mk,rk,market_data(rk)];
                rk=rk+1;
            end
            row_ind = row_ind+segment_competitors(mk);
        end       
        dlmwrite(outfile_fn,freq_results_mat,'delimiter',',','precision','%.4f')
        %calculate loss metric chosen
        freq_results_mat = zeros(sum(segment_competitors),3);
        %row index
        row_ind =1;
        %for each market...
        for mk=1:numel(segment_competitors)
            market_data = Market_freqs{mk};
            %for each competitor in market...        
            rk=1;
            for row=row_ind:row_ind+segment_competitors(mk) - 1
                freq_results_mat(row,1)=market_data(rk);      
                rk=rk+1;
            end
            row_ind = row_ind+segment_competitors(mk);
        end
        freq_results_mat(:,2)=empirical_freqs;
        freq_results_mat = freq_results_mat(logical(MAPE_incl),:);
        if (loss_metric==1) %calculuate MAPE
            loss= sum(abs(freq_results_mat(:,1) - freq_results_mat(:,2)))./sum(freq_results_mat(:,2));
        else %calculate negative R^2
            loss = -(1-sum((freq_results_mat(:,1)-freq_results_mat(:,2)).^2)./sum((freq_results_mat(:,2)-repmat(mean(freq_results_mat(:,2)),numel(freq_results_mat(:,2)),1)).^2));
        end
    else
        %calculate loss metric chosen
        freq_results_mat = zeros(sum(segment_competitors),3);
        %row index
        row_ind =1;
        %for each market...
        for mk=1:numel(segment_competitors)
            market_data = Market_freqs{mk};
            %for each competitor in market...        
            rk=1;
            for row=row_ind:row_ind+segment_competitors(mk) - 1
                freq_results_mat(row,1)=market_data(rk);      
                rk=rk+1;
            end
            row_ind = row_ind+segment_competitors(mk);
        end
        freq_results_mat(:,2)=empirical_freqs;
        freq_results_mat = freq_results_mat(logical(MAPE_incl),:);
        if (loss_metric==1) %calculuate MAPE
            loss= sum(abs(freq_results_mat(:,1) - freq_results_mat(:,2)))./sum(freq_results_mat(:,2));
        else %calculate negative R^2
            loss = -(1-sum((freq_results_mat(:,1)-freq_results_mat(:,2)).^2)./sum((freq_results_mat(:,2)-repmat(mean(freq_results_mat(:,2)),numel(freq_results_mat(:,2)),1)).^2));
        end
    end
        

    
    